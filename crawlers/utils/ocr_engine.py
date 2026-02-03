"""
OCR Engine for image text extraction.

This module provides OCR capabilities using EasyOCR with
image preprocessing for optimal text recognition.
"""

import io
import logging
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Tuple, Union
from pathlib import Path

import numpy as np
from PIL import Image, ImageEnhance, ImageFilter

logger = logging.getLogger(__name__)


@dataclass
class OCRResult:
    """Result of OCR operation."""
    success: bool
    text: str = ""
    raw_blocks: List[Dict[str, Any]] = field(default_factory=list)
    confidence: float = 0.0
    language_detected: Optional[str] = None
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'success': self.success,
            'text': self.text,
            'raw_blocks': self.raw_blocks,
            'confidence': self.confidence,
            'language_detected': self.language_detected,
            'error_message': self.error_message,
            'metadata': self.metadata
        }


class OCREngine:
    """
    OCR Engine using EasyOCR with image preprocessing.

    Supports Korean and English text extraction with
    various preprocessing options for improved accuracy.
    """

    def __init__(
        self,
        languages: List[str] = None,
        gpu: bool = False,
        model_storage_directory: Optional[str] = None,
        download_enabled: bool = True
    ):
        """
        Initialize OCR Engine.

        Args:
            languages: List of language codes (default: ['ko', 'en'])
            gpu: Use GPU acceleration if available
            model_storage_directory: Custom model storage path
            download_enabled: Allow model download if not present
        """
        self.languages = languages or ['ko', 'en']
        self.gpu = gpu
        self.model_storage_directory = model_storage_directory
        self.download_enabled = download_enabled
        self._reader = None

    @property
    def reader(self):
        """Lazy initialization of EasyOCR reader."""
        if self._reader is None:
            import easyocr

            kwargs = {
                'lang_list': self.languages,
                'gpu': self.gpu,
                'download_enabled': self.download_enabled
            }

            if self.model_storage_directory:
                kwargs['model_storage_directory'] = self.model_storage_directory

            logger.info(f"Initializing EasyOCR with languages: {self.languages}")
            self._reader = easyocr.Reader(**kwargs)

        return self._reader

    def preprocess_image(
        self,
        image: Image.Image,
        enhance_contrast: bool = True,
        denoise: bool = True,
        binarize: bool = False,
        resize_factor: float = 1.0,
        deskew: bool = False
    ) -> Image.Image:
        """
        Preprocess image for better OCR accuracy.

        Args:
            image: PIL Image object
            enhance_contrast: Increase contrast
            denoise: Apply denoising filter
            binarize: Convert to black and white
            resize_factor: Scale factor (>1 for upscale)
            deskew: Attempt to correct skew

        Returns:
            Preprocessed PIL Image
        """
        # Convert to RGB if necessary
        if image.mode != 'RGB':
            image = image.convert('RGB')

        # Resize if needed
        if resize_factor != 1.0:
            new_size = (
                int(image.width * resize_factor),
                int(image.height * resize_factor)
            )
            image = image.resize(new_size, Image.Resampling.LANCZOS)

        # Denoise
        if denoise:
            image = image.filter(ImageFilter.MedianFilter(size=3))

        # Enhance contrast
        if enhance_contrast:
            enhancer = ImageEnhance.Contrast(image)
            image = enhancer.enhance(1.5)

            # Also enhance sharpness
            sharpener = ImageEnhance.Sharpness(image)
            image = sharpener.enhance(1.3)

        # Binarize (convert to black and white)
        if binarize:
            image = image.convert('L')  # Grayscale
            image = image.point(lambda x: 0 if x < 128 else 255, '1')
            image = image.convert('RGB')

        # Deskew (simple rotation correction)
        if deskew:
            image = self._auto_deskew(image)

        return image

    def _auto_deskew(self, image: Image.Image) -> Image.Image:
        """
        Attempt to auto-correct image skew.

        Args:
            image: PIL Image object

        Returns:
            Deskewed image
        """
        try:
            import cv2

            # Convert to OpenCV format
            img_array = np.array(image)
            gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)

            # Edge detection
            edges = cv2.Canny(gray, 50, 150, apertureSize=3)

            # Hough line detection
            lines = cv2.HoughLines(edges, 1, np.pi/180, 200)

            if lines is not None and len(lines) > 0:
                # Calculate average angle
                angles = []
                for line in lines[:10]:  # Use first 10 lines
                    rho, theta = line[0]
                    angle = (theta * 180 / np.pi) - 90
                    if -45 < angle < 45:  # Only consider reasonable angles
                        angles.append(angle)

                if angles:
                    avg_angle = np.median(angles)
                    if abs(avg_angle) > 0.5:  # Only rotate if significant skew
                        image = image.rotate(avg_angle, expand=True, fillcolor='white')

            return image

        except Exception as e:
            logger.warning(f"Deskew failed: {e}")
            return image

    def extract_text(
        self,
        image_source: Union[str, bytes, Image.Image, np.ndarray],
        preprocess: bool = True,
        detail: int = 1,
        paragraph: bool = True,
        min_confidence: float = 0.3,
        **preprocess_kwargs
    ) -> OCRResult:
        """
        Extract text from image.

        Args:
            image_source: Image path, bytes, PIL Image, or numpy array
            preprocess: Apply preprocessing
            detail: OCR detail level (0=simple, 1=detailed)
            paragraph: Merge text into paragraphs
            min_confidence: Minimum confidence threshold
            **preprocess_kwargs: Additional preprocessing options

        Returns:
            OCRResult with extracted text
        """
        try:
            # Load image
            image = self._load_image(image_source)

            if image is None:
                return OCRResult(
                    success=False,
                    error_message="Failed to load image"
                )

            # Store original size
            original_size = image.size

            # Preprocess if enabled
            if preprocess:
                image = self.preprocess_image(image, **preprocess_kwargs)

            # Convert to numpy array for EasyOCR
            img_array = np.array(image)

            # Perform OCR
            results = self.reader.readtext(
                img_array,
                detail=detail,
                paragraph=paragraph
            )

            # Process results
            if detail == 0:
                # Simple mode: just text
                text = ' '.join(results) if results else ''
                raw_blocks = [{'text': r} for r in results]
                avg_confidence = 1.0
            else:
                # Detailed mode: [[bbox, text, confidence], ...]
                raw_blocks = []
                texts = []
                confidences = []

                for item in results:
                    if len(item) >= 3:
                        bbox, text, conf = item[0], item[1], item[2]
                    elif len(item) == 2:
                        bbox, text = item[0], item[1]
                        conf = 1.0
                    else:
                        continue

                    if conf >= min_confidence:
                        raw_blocks.append({
                            'bbox': bbox if isinstance(bbox, list) else list(bbox),
                            'text': text,
                            'confidence': float(conf)
                        })
                        texts.append(text)
                        confidences.append(conf)

                text = '\n'.join(texts) if texts else ''
                avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0

            # Detect primary language
            language_detected = self._detect_language(text)

            return OCRResult(
                success=True,
                text=text,
                raw_blocks=raw_blocks,
                confidence=avg_confidence,
                language_detected=language_detected,
                metadata={
                    'original_size': original_size,
                    'preprocessed': preprocess,
                    'block_count': len(raw_blocks)
                }
            )

        except Exception as e:
            logger.error(f"OCR extraction failed: {e}")
            return OCRResult(
                success=False,
                error_message=str(e)
            )

    def extract_structured_news(
        self,
        image_source: Union[str, bytes, Image.Image, np.ndarray],
        **kwargs
    ) -> Dict[str, Any]:
        """
        Extract structured news content from image.

        Attempts to identify title, date, body text, etc.

        Args:
            image_source: Image source
            **kwargs: Additional OCR options

        Returns:
            Structured news data
        """
        result = self.extract_text(image_source, paragraph=False, **kwargs)

        if not result.success:
            return {'success': False, 'error': result.error_message}

        # Analyze block positions and sizes to identify structure
        structured = {
            'success': True,
            'title': '',
            'date': '',
            'author': '',
            'body': '',
            'raw_text': result.text,
            'confidence': result.confidence
        }

        if not result.raw_blocks:
            structured['body'] = result.text
            return structured

        # Sort blocks by position (top to bottom, left to right)
        sorted_blocks = sorted(
            result.raw_blocks,
            key=lambda x: (
                min(p[1] for p in x['bbox']) if x.get('bbox') else 0,
                min(p[0] for p in x['bbox']) if x.get('bbox') else 0
            )
        )

        # Heuristics for news structure
        # Title: Usually first block with larger font (wider bbox)
        # Date: Contains date patterns, often near top
        # Body: Rest of the text

        import re
        date_pattern = re.compile(
            r'(\d{4}[-./년]\d{1,2}[-./월]\d{1,2}일?|'
            r'\d{1,2}[-./월]\d{1,2}[-./일]?\s*\d{4}|'
            r'\d{4}\.\d{1,2}\.\d{1,2})',
            re.UNICODE
        )

        title_candidates = []
        date_text = ''
        body_parts = []

        for i, block in enumerate(sorted_blocks):
            text = block.get('text', '').strip()
            if not text:
                continue

            # Check for date
            date_match = date_pattern.search(text)
            if date_match and not date_text:
                date_text = date_match.group()
                # Remove date from text if it's the only content
                if len(text) < 30:
                    continue

            # First few blocks might be title
            if i < 3 and len(text) < 100:
                title_candidates.append(text)
            else:
                body_parts.append(text)

        # Determine title (longest short text at top, or first text)
        if title_candidates:
            structured['title'] = max(title_candidates, key=len)
            # Add remaining candidates to body
            for t in title_candidates:
                if t != structured['title']:
                    body_parts.insert(0, t)

        structured['date'] = date_text
        structured['body'] = '\n'.join(body_parts)

        return structured

    def _load_image(
        self,
        source: Union[str, bytes, Image.Image, np.ndarray]
    ) -> Optional[Image.Image]:
        """
        Load image from various sources.

        Args:
            source: Image path, bytes, PIL Image, or numpy array

        Returns:
            PIL Image or None
        """
        try:
            if isinstance(source, Image.Image):
                return source

            if isinstance(source, np.ndarray):
                return Image.fromarray(source)

            if isinstance(source, bytes):
                return Image.open(io.BytesIO(source))

            if isinstance(source, str):
                # Could be file path or URL
                if source.startswith(('http://', 'https://')):
                    return self._load_from_url(source)
                else:
                    return Image.open(source)

            return None

        except Exception as e:
            logger.error(f"Failed to load image: {e}")
            return None

    def _load_from_url(self, url: str) -> Optional[Image.Image]:
        """
        Load image from URL.

        Args:
            url: Image URL

        Returns:
            PIL Image or None
        """
        import requests

        try:
            response = requests.get(url, timeout=30, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            response.raise_for_status()
            return Image.open(io.BytesIO(response.content))
        except Exception as e:
            logger.error(f"Failed to load image from URL {url}: {e}")
            return None

    def _detect_language(self, text: str) -> str:
        """
        Detect primary language of text.

        Args:
            text: Text to analyze

        Returns:
            Language code ('ko', 'en', 'mixed')
        """
        if not text:
            return 'unknown'

        # Count Korean and ASCII characters
        korean_count = sum(1 for c in text if '\uac00' <= c <= '\ud7a3')
        ascii_count = sum(1 for c in text if c.isascii() and c.isalpha())

        total = korean_count + ascii_count
        if total == 0:
            return 'unknown'

        korean_ratio = korean_count / total

        if korean_ratio > 0.7:
            return 'ko'
        elif korean_ratio < 0.3:
            return 'en'
        else:
            return 'mixed'

    def batch_extract(
        self,
        images: List[Union[str, bytes, Image.Image]],
        **kwargs
    ) -> List[OCRResult]:
        """
        Extract text from multiple images.

        Args:
            images: List of image sources
            **kwargs: OCR options

        Returns:
            List of OCRResult objects
        """
        results = []
        for img in images:
            result = self.extract_text(img, **kwargs)
            results.append(result)
        return results

    def extract_table_text(
        self,
        image_source: Union[str, bytes, Image.Image],
        **kwargs
    ) -> List[List[str]]:
        """
        Extract text organized in table structure.

        Args:
            image_source: Image source
            **kwargs: OCR options

        Returns:
            2D list representing table structure
        """
        result = self.extract_text(image_source, paragraph=False, **kwargs)

        if not result.success or not result.raw_blocks:
            return []

        # Group blocks by row (similar Y coordinates)
        blocks = result.raw_blocks

        # Calculate row groupings
        rows = []
        row_threshold = 20  # Pixels tolerance for same row

        sorted_blocks = sorted(
            blocks,
            key=lambda x: min(p[1] for p in x['bbox']) if x.get('bbox') else 0
        )

        current_row = []
        current_y = None

        for block in sorted_blocks:
            if not block.get('bbox'):
                continue

            y = min(p[1] for p in block['bbox'])

            if current_y is None or abs(y - current_y) <= row_threshold:
                current_row.append(block)
                current_y = y if current_y is None else (current_y + y) / 2
            else:
                if current_row:
                    # Sort row by X coordinate
                    current_row.sort(key=lambda x: min(p[0] for p in x['bbox']))
                    rows.append([b['text'] for b in current_row])
                current_row = [block]
                current_y = y

        # Don't forget last row
        if current_row:
            current_row.sort(key=lambda x: min(p[0] for p in x['bbox']))
            rows.append([b['text'] for b in current_row])

        return rows
