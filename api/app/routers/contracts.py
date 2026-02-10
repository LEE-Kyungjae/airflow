"""
Data Contracts Router - 데이터 품질 계약 관리 API

엔드포인트:
- GET /: 계약 목록 조회
- POST /: 템플릿으로 계약 생성
- GET /templates: 사용 가능한 템플릿 목록
- GET /{contract_id}: 계약 상세 조회
- DELETE /{contract_id}: 계약 삭제
- POST /validate: 데이터 검증
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query, Depends, Body
from pydantic import BaseModel, Field

from app.services.mongo_service import MongoService
from app.services.data_contracts import (
    DataContract,
    ContractBuilder,
    ContractTemplates,
    ContractStatus,
    ContractValidationResult,
    ContractValidator,
    ContractRegistry,
    ValidationConfig,
    ContractReporter,
)
from app.core import get_logger
from app.auth.dependencies import require_auth, require_scope, require_admin, AuthContext

logger = get_logger(__name__)
router = APIRouter()


# ============== Models ==============

class ContractCreateRequest(BaseModel):
    """계약 생성 요청"""
    template: str = Field(..., description="템플릿 이름 (news_articles, financial_data, stock_prices, exchange_rates, generic_table)")
    source_id: str = Field(..., description="소스 ID")
    name: Optional[str] = Field(None, description="계약 이름 (자동 생성)")
    description: Optional[str] = Field(None, description="계약 설명")
    required_columns: Optional[List[str]] = Field(None, description="필수 컬럼 (generic_table 템플릿용)")


class ContractResponse(BaseModel):
    """계약 응답"""
    id: str
    name: str
    description: str
    source_id: Optional[str]
    version: str
    status: str
    expectations_count: int
    created_at: Optional[str]
    updated_at: Optional[str]


class TemplateInfo(BaseModel):
    """템플릿 정보"""
    name: str
    description: str
    data_category: str


class ValidationRequest(BaseModel):
    """검증 요청"""
    contract_id: str = Field(..., description="계약 ID")
    data: List[Dict[str, Any]] = Field(..., description="검증할 데이터")
    source_id: str = Field(..., description="소스 ID")
    run_id: Optional[str] = Field(None, description="실행 ID")


class ValidationResponse(BaseModel):
    """검증 응답"""
    success: bool
    contract_name: str
    contract_version: str
    success_rate: float
    total_expectations: int
    passed: int
    failed: int
    critical_failures: int
    error_failures: int
    warning_failures: int
    run_time: str
    validation_id: Optional[str]


# ============== Dependencies ==============

def get_mongo():
    """MongoDB 서비스 의존성"""
    mongo = MongoService()
    try:
        yield mongo
    finally:
        mongo.close()


def get_contract_registry(mongo: MongoService = Depends(get_mongo)):
    """계약 레지스트리 의존성"""
    return ContractRegistry(mongo_service=mongo)


def get_contract_validator(mongo: MongoService = Depends(get_mongo)):
    """계약 검증기 의존성"""
    return ContractValidator(mongo_service=mongo)


def get_contract_reporter(mongo: MongoService = Depends(get_mongo)):
    """계약 리포터 의존성"""
    return ContractReporter(mongo_service=mongo)


# ============== Endpoints ==============

@router.get("/", response_model=List[ContractResponse])
async def list_contracts(
    source_id: Optional[str] = Query(None, description="소스 ID 필터"),
    status: Optional[str] = Query(None, description="상태 필터 (draft, active, deprecated, archived)"),
    limit: int = Query(50, ge=1, le=200, description="최대 조회 개수"),
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_auth)
):
    """
    Get list of all data contracts.

    데이터 계약 목록을 조회합니다.
    소스 ID나 상태로 필터링할 수 있습니다.
    """
    try:
        # 필터 구성
        filter_dict = {}
        if source_id:
            filter_dict["source_id"] = source_id
        if status:
            filter_dict["status"] = status

        # MongoDB에서 직접 조회
        cursor = mongo.db.data_contracts.find(filter_dict).limit(limit)

        # 응답 변환
        response = []
        for doc in cursor:
            response.append(ContractResponse(
                id=str(doc.get("contract_id", "")),
                name=doc.get("name", ""),
                description=doc.get("description", ""),
                source_id=doc.get("source_id"),
                version=doc.get("version", "1.0.0"),
                status=doc.get("status", "draft"),
                expectations_count=len(doc.get("expectations", [])),
                created_at=doc.get("created_at"),
                updated_at=doc.get("updated_at")
            ))

        logger.info(f"Listed {len(response)} contracts with filters: {filter_dict}")
        return response

    except Exception as e:
        logger.error(f"Failed to list contracts: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list contracts: {str(e)}")


@router.post("/", response_model=ContractResponse, status_code=201)
async def create_contract(
    request: ContractCreateRequest,
    registry: ContractRegistry = Depends(get_contract_registry),
    auth: AuthContext = Depends(require_scope("write"))
):
    """
    Create a new data contract from template.

    템플릿을 사용하여 새로운 데이터 계약을 생성합니다.
    """
    try:
        # 템플릿 선택
        template_map = {
            "news_articles": ContractTemplates.news_articles,
            "financial_data": ContractTemplates.financial_data,
            "stock_prices": ContractTemplates.stock_prices,
            "exchange_rates": ContractTemplates.exchange_rates,
            "generic_table": lambda **kw: ContractTemplates.generic_table(
                required_columns=request.required_columns,
                **kw
            )
        }

        if request.template not in template_map:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid template: {request.template}. Available: {list(template_map.keys())}"
            )

        # 계약 생성
        template_func = template_map[request.template]
        contract = template_func(source_id=request.source_id)

        # 이름/설명 오버라이드
        if request.name:
            contract.name = request.name
        if request.description:
            contract.description = request.description

        # 레지스트리에 등록
        await registry.register(contract)

        contract_dict = contract.to_dict()
        response = ContractResponse(
            id=contract_dict.get("id", ""),
            name=contract.name,
            description=contract.description,
            source_id=contract.source_id,
            version=contract.version,
            status=contract.status.value,
            expectations_count=len(contract.expectations),
            created_at=contract_dict.get("created_at"),
            updated_at=contract_dict.get("updated_at")
        )

        logger.info(f"Created contract '{contract.name}' from template '{request.template}' for source {request.source_id}")
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create contract: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create contract: {str(e)}")


@router.get("/templates", response_model=List[TemplateInfo])
async def list_templates(
    auth: AuthContext = Depends(require_auth)
):
    """
    Get list of available contract templates.

    사용 가능한 계약 템플릿 목록을 조회합니다.
    """
    templates = [
        TemplateInfo(
            name="news_articles",
            description="뉴스 기사 데이터 품질 계약 (title, content, url, published_at 등)",
            data_category="news_article"
        ),
        TemplateInfo(
            name="financial_data",
            description="금융 데이터 품질 계약 (symbol, date, value 등)",
            data_category="financial_data"
        ),
        TemplateInfo(
            name="stock_prices",
            description="주식 가격 데이터 품질 계약 (symbol, date, open, high, low, close, volume)",
            data_category="stock_price"
        ),
        TemplateInfo(
            name="exchange_rates",
            description="환율 데이터 품질 계약 (currency_pair, rate, date)",
            data_category="exchange_rate"
        ),
        TemplateInfo(
            name="generic_table",
            description="일반 테이블 데이터 품질 계약 (사용자 정의 필수 컬럼)",
            data_category="generic"
        )
    ]

    return templates


@router.get("/{contract_id}", response_model=Dict[str, Any])
async def get_contract(
    contract_id: str,
    registry: ContractRegistry = Depends(get_contract_registry),
    auth: AuthContext = Depends(require_auth)
):
    """
    Get contract details by ID.

    계약 상세 정보를 조회합니다.
    """
    try:
        contract = await registry.get_by_id(contract_id)

        if not contract:
            raise HTTPException(status_code=404, detail=f"Contract not found: {contract_id}")

        logger.info(f"Retrieved contract {contract_id}")
        return contract.to_dict()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get contract {contract_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get contract: {str(e)}")


@router.delete("/{contract_id}", status_code=204)
async def delete_contract(
    contract_id: str,
    mongo: MongoService = Depends(get_mongo),
    auth: AuthContext = Depends(require_scope("write"))
):
    """
    Delete a contract.

    계약을 삭제합니다.
    """
    try:
        # 계약 존재 확인
        existing = mongo.db.data_contracts.find_one({"contract_id": contract_id})
        if not existing:
            raise HTTPException(status_code=404, detail=f"Contract not found: {contract_id}")

        # 삭제 실행
        result = mongo.db.data_contracts.delete_one({"contract_id": contract_id})

        if result.deleted_count == 0:
            raise HTTPException(status_code=500, detail="Failed to delete contract")

        logger.info(f"Deleted contract {contract_id}")
        return None

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete contract {contract_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete contract: {str(e)}")


@router.post("/validate", response_model=ValidationResponse)
async def validate_data(
    request: ValidationRequest,
    validator: ContractValidator = Depends(get_contract_validator),
    registry: ContractRegistry = Depends(get_contract_registry),
    auth: AuthContext = Depends(require_scope("write"))
):
    """
    Validate data against a contract.

    데이터를 계약에 따라 검증합니다.
    """
    try:
        # 계약 조회
        contract = await registry.get_by_id(request.contract_id)
        if not contract:
            raise HTTPException(status_code=404, detail=f"Contract not found: {request.contract_id}")

        # 검증 실행
        validated = await validator.validate(
            contract=contract,
            data=request.data,
            source_id=request.source_id,
            run_id=request.run_id
        )

        result = validated.validation_result

        # 응답 구성
        response = ValidationResponse(
            success=result.success,
            contract_name=result.contract_name,
            contract_version=result.contract_version,
            success_rate=round(result.success_rate, 2),
            total_expectations=len(result.results),
            passed=result.passed_count,
            failed=result.failed_count,
            critical_failures=len(result.critical_failures),
            error_failures=len(result.error_failures),
            warning_failures=len(result.warning_failures),
            run_time=result.run_time.isoformat(),
            validation_id=None  # ValidatedData doesn't have validation_id field
        )

        logger.info(
            f"Validated {len(request.data)} records against contract {request.contract_id}: "
            f"success={result.success}, success_rate={result.success_rate:.2f}%"
        )

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to validate data: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to validate data: {str(e)}")
