"""
OCR Crawler for image-based content extraction.

This module provides crawlers for extracting text from images,
particularly optimized for news image crawling with AI-powered
error correction and text structuring.
"""

import logging
import re
from typing import Dict, Any, List, Optional, Union
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

from .base_crawler import BaseCrawler, CrawlResult
from .utils.ocr_engine import OCREngine, OCRResult
from .utils.ai_text_refiner import AITextRefiner, OCRPipeline

logger = logging.getLogger(__name__)


class OCRCrawler(BaseCrawler):
    """
    Crawler for extracting text from images using OCR.

    Features:
    - Multi-language OCR support (Korean, English)
    - Image preprocessing for improved accuracy
    - AI-powered text correction and structuring
    - News image specialization
    """

    def __init__(
        self,
        url: str,
        ocr_languages: List[str] = None,
        use_gpu: bool = False,
        enable_ai_refinement: bool = True,
        openai_api_key: Optional[str] = None,
        openai_model: str = "gpt-4o-mini",
        image_selectors: Optional[List[str]] = None,
        content_type: str = "auto",
        preprocess_options: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        """
        Initialize OCR Crawler.

        Args:
            url: Target URL containing images
            ocr_languages: Languages for OCR (default: ['ko', 'en'])
            use_gpu: Use GPU acceleration for OCR
            enable_ai_refinement: Enable AI text correction
            openai_api_key: OpenAI API key for AI refinement
            openai_model: GPT model for AI refinement
            image_selectors: CSS selectors for finding images
            content_type: Content type hint ('news', 'table', 'general', 'auto')
            preprocess_options: Image preprocessing options
            **kwargs: Additional arguments for BaseCrawler
        """
        super().__init__(url, **kwargs)

        self.ocr_languages = ocr_languages or ['ko', 'en']
        self.use_gpu = use_gpu
        self.enable_ai_refinement = enable_ai_refinement
        self.openai_api_key = openai_api_key
        self.openai_model = openai_model
        self.image_selectors = image_selectors or [
            'img.news-image',
            'img.article-image',
            'img[src*="news"]',
            'img[src*="article"]',
            '.article-body img',
            '.news-content img',
            'article img',
            '.content img'
        ]
        self.content_type = content_type
        self.preprocess_options = preprocess_options or {
            'enhance_contrast': True,
            'denoise': True,
            'resize_factor': 1.5
        }

        # Lazy initialization
        self._ocr_engine: Optional[OCREngine] = None
        self._ai_refiner: Optional[AITextRefiner] = None
        self._pipeline: Optional[OCRPipeline] = None

    @property
    def ocr_engine(self) -> OCREngine:
        """Get or create OCR engine."""
        if self._ocr_engine is None:
            self._ocr_engine = OCREngine(
                languages=self.ocr_languages,
                gpu=self.use_gpu
            )
        return self._ocr_engine

    @property
    def ai_refiner(self) -> Optional[AITextRefiner]:
        """Get or create AI refiner."""
        if self._ai_refiner is None and self.enable_ai_refinement:
            self._ai_refiner = AITextRefiner(
                api_key=self.openai_api_key,
                model=self.openai_model
            )
        return self._ai_refiner

    @property
    def pipeline(self) -> OCRPipeline:
        """Get or create OCR pipeline."""
        if self._pipeline is None:
            self._pipeline = OCRPipeline(
                ocr_languages=self.ocr_languages,
                use_gpu=self.use_gpu,
                openai_api_key=self.openai_api_key,
                openai_model=self.openai_model
            )
        return self._pipeline

    def crawl(self, fields: List[Dict[str, str]]) -> CrawlResult:
        """
        Crawl images from URL and extract text using OCR.

        Args:
            fields: Field definitions (used for content type hints)

        Returns:
            CrawlResult with extracted data
        """
        try:
            # Fetch the page
            response = self.fetch_url()
            encoding = self.detect_encoding(response)
            response.encoding = encoding
            html_content = response.text

            # Parse HTML and find images
            soup = BeautifulSoup(html_content, 'lxml')
            image_urls = self._find_images(soup)

            if not image_urls:
                return CrawlResult(
                    success=False,
                    error_code='E002',
                    error_message='No images found with specified selectors',
                    html_snapshot=html_content[:5000]
                )

            # Process each image
            extracted_data = []
            total_confidence = 0.0

            for img_url in image_urls:
                result = self._process_image(img_url, fields)
                if result:
                    extracted_data.append(result)
                    total_confidence += result.get('confidence', 0)

            if not extracted_data:
                return CrawlResult(
                    success=False,
                    error_code='E010',
                    error_message='OCR extraction failed for all images',
                    html_snapshot=html_content[:5000]
                )

            avg_confidence = total_confidence / len(extracted_data) if extracted_data else 0

            return CrawlResult(
                success=True,
                data=extracted_data,
                record_count=len(extracted_data),
                metadata={
                    'images_processed': len(image_urls),
                    'images_successful': len(extracted_data),
                    'average_confidence': avg_confidence,
                    'ai_refinement_enabled': self.enable_ai_refinement
                }
            )

        except Exception as e:
            logger.error(f"Error in OCR crawl: {e}")
            return CrawlResult(
                success=False,
                error_code='E010',
                error_message=str(e)
            )

    def _find_images(self, soup: BeautifulSoup) -> List[str]:
        """
        Find image URLs in the page.

        Args:
            soup: BeautifulSoup object

        Returns:
            List of absolute image URLs
        """
        image_urls = []
        seen = set()

        for selector in self.image_selectors:
            try:
                elements = soup.select(selector)
                for img in elements:
                    # Get image URL
                    src = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
                    if not src:
                        continue

                    # Convert to absolute URL
                    if not src.startswith(('http://', 'https://')):
                        src = urljoin(self.url, src)

                    # Skip duplicates and data URIs
                    if src in seen or src.startswith('data:'):
                        continue

                    # Skip small images (likely icons)
                    width = img.get('width', '').replace('px', '')
                    height = img.get('height', '').replace('px', '')
                    try:
                        if width and int(width) < 100:
                            continue
                        if height and int(height) < 100:
                            continue
                    except ValueError:
                        pass

                    seen.add(src)
                    image_urls.append(src)

            except Exception as e:
                logger.warning(f"Error with selector {selector}: {e}")
                continue

        return image_urls

    def _process_image(
        self,
        image_url: str,
        fields: List[Dict[str, str]]
    ) -> Optional[Dict[str, Any]]:
        """
        Process a single image through OCR pipeline.

        Args:
            image_url: Image URL
            fields: Field definitions

        Returns:
            Extracted data or None
        """
        try:
            # Determine content type
            content_type = self.content_type
            if content_type == 'auto':
                content_type = self._detect_content_type_from_fields(fields)

            # Process through pipeline
            if content_type == 'news':
                result = self.pipeline.process_news_image(
                    image_url,
                    source_name=urlparse(self.url).netloc,
                    **self.preprocess_options
                )
            else:
                result = self.pipeline.process_image(
                    image_url,
                    content_type=content_type,
                    preprocess=True,
                    refine=self.enable_ai_refinement,
                    **self.preprocess_options
                )

            if not result.get('success'):
                logger.warning(f"Failed to process image {image_url}: {result.get('error')}")
                return None

            # Build output
            output = {
                'source_url': image_url,
                'confidence': result.get('ai_confidence') or result.get('ocr_confidence', 0)
            }

            # Add structured data based on content type
            if content_type == 'news' and result.get('structured_news'):
                output.update(result['structured_news'])
            elif result.get('final_data'):
                output.update(result['final_data'])

            # Add corrections info if available
            if result.get('corrections'):
                output['_corrections'] = result['corrections']
                output['_corrections_count'] = len(result['corrections'])

            return output

        except Exception as e:
            logger.error(f"Error processing image {image_url}: {e}")
            return None

    def _detect_content_type_from_fields(
        self,
        fields: List[Dict[str, str]]
    ) -> str:
        """
        Detect content type from field definitions.

        Args:
            fields: Field definitions

        Returns:
            Content type string
        """
        field_names = [f.get('name', '').lower() for f in fields]

        news_fields = ['title', 'headline', 'date', 'author', 'body', 'content']
        table_fields = ['row', 'column', 'cell', 'header']

        news_count = sum(1 for f in field_names if any(n in f for n in news_fields))
        table_count = sum(1 for f in field_names if any(t in f for t in table_fields))

        if news_count >= 2:
            return 'news'
        elif table_count >= 2:
            return 'table'
        else:
            return 'general'

    def crawl_single_image(
        self,
        image_source: Union[str, bytes],
        content_type: str = 'auto'
    ) -> CrawlResult:
        """
        Crawl a single image directly.

        Args:
            image_source: Image URL, path, or bytes
            content_type: Content type hint

        Returns:
            CrawlResult with extracted data
        """
        try:
            if content_type == 'news':
                result = self.pipeline.process_news_image(
                    image_source,
                    **self.preprocess_options
                )
            else:
                result = self.pipeline.process_image(
                    image_source,
                    content_type=content_type,
                    preprocess=True,
                    refine=self.enable_ai_refinement,
                    **self.preprocess_options
                )

            if not result.get('success'):
                return CrawlResult(
                    success=False,
                    error_code='E010',
                    error_message=result.get('error', 'OCR extraction failed')
                )

            data = {
                'confidence': result.get('ai_confidence') or result.get('ocr_confidence', 0)
            }

            if content_type == 'news' and result.get('structured_news'):
                data.update(result['structured_news'])
            elif result.get('final_data'):
                data.update(result['final_data'])

            return CrawlResult(
                success=True,
                data=[data],
                record_count=1,
                metadata={
                    'ai_refinement_enabled': self.enable_ai_refinement,
                    'corrections_count': len(result.get('corrections', []))
                }
            )

        except Exception as e:
            logger.error(f"Error in single image crawl: {e}")
            return CrawlResult(
                success=False,
                error_code='E010',
                error_message=str(e)
            )


class NewsImageCrawler(OCRCrawler):
    """
    Specialized crawler for news images.

    Optimized for extracting structured news data from
    news article images with high accuracy.
    """

    def __init__(self, url: str, **kwargs):
        """Initialize with news-optimized settings."""
        # News-specific image selectors
        kwargs.setdefault('image_selectors', [
            'img.news-image',
            'img.article-image',
            'img.headline-image',
            '.article-body img',
            '.news-content img',
            '.story-image img',
            'figure.article img',
            '.main-article img',
            'article img[src*="news"]',
            'article img[src*="article"]'
        ])
        kwargs.setdefault('content_type', 'news')
        kwargs.setdefault('enable_ai_refinement', True)

        # Optimized preprocessing for news images
        kwargs.setdefault('preprocess_options', {
            'enhance_contrast': True,
            'denoise': True,
            'resize_factor': 2.0,  # Larger resize for better text clarity
            'binarize': False,
            'deskew': True
        })

        super().__init__(url, **kwargs)

    def crawl(self, fields: List[Dict[str, str]] = None) -> CrawlResult:
        """
        Crawl news images with news-specific field defaults.

        Args:
            fields: Field definitions (uses defaults if not provided)

        Returns:
            CrawlResult with structured news data
        """
        if fields is None:
            fields = [
                {'name': 'headline', 'data_type': 'string'},
                {'name': 'subheadline', 'data_type': 'string'},
                {'name': 'date', 'data_type': 'date'},
                {'name': 'author', 'data_type': 'string'},
                {'name': 'publisher', 'data_type': 'string'},
                {'name': 'body', 'data_type': 'string'},
                {'name': 'keywords', 'data_type': 'list'}
            ]

        return super().crawl(fields)


class TableImageCrawler(OCRCrawler):
    """
    Specialized crawler for table images.

    Optimized for extracting tabular data from images
    with structure preservation.
    """

    def __init__(self, url: str, **kwargs):
        """Initialize with table-optimized settings."""
        kwargs.setdefault('content_type', 'table')
        kwargs.setdefault('preprocess_options', {
            'enhance_contrast': True,
            'denoise': True,
            'resize_factor': 2.0,
            'binarize': True,  # Better for table lines
            'deskew': True
        })

        super().__init__(url, **kwargs)

    def crawl_table(
        self,
        image_source: Union[str, bytes]
    ) -> Dict[str, Any]:
        """
        Crawl table image and extract structured data.

        Args:
            image_source: Image source

        Returns:
            Structured table data
        """
        # Extract table rows using OCR
        rows = self.ocr_engine.extract_table_text(
            image_source,
            preprocess=True,
            **self.preprocess_options
        )

        if not rows:
            return {
                'success': False,
                'error': 'No table structure detected'
            }

        # Refine with AI if enabled
        if self.enable_ai_refinement and self.ai_refiner:
            result = self.ai_refiner.extract_table_structure(rows)
            return result
        else:
            return {
                'success': True,
                'headers': rows[0] if rows else [],
                'rows': rows[1:] if len(rows) > 1 else [],
                'raw_rows': rows
            }
