"""
PDF parser component for extracting text from PDF files.

This module provides functionality to extract text content from PDF files
using PyMuPDF (fitz).
"""

import io
import logging
from typing import Optional, Tuple

import pymupdf


class PDFParser:
    """PDF parser for extracting text from PDF files."""
    
    def __init__(self):
        """Initialize the PDF parser."""
        self.logger = logging.getLogger(__name__)
    
    
    def parse_pdf_content(self, pdf_bytes: bytes) -> Tuple[Optional[str], str]:
        """
        Extract text content from PDF bytes.
        
        Args:
            pdf_bytes: PDF file bytes
            
        Returns:
            Tuple of (extracted_text, status) where status is 'success', 'failed', or 'empty'
        """
        try:
            # Open PDF from bytes
            pdf_document = pymupdf.open(stream=pdf_bytes, filetype="pdf")
            
            extracted_text = ""
            total_pages = pdf_document.page_count
            
            # Extract text from all pages
            for page_num in range(total_pages):
                page = pdf_document[page_num]
                page_text = page.get_text()
                extracted_text += page_text + "\n"
            
            pdf_document.close()
            
            # Clean up the text
            extracted_text = self._clean_text(extracted_text)
            
            if not extracted_text.strip():
                self.logger.warning("PDF contains no extractable text")
                return None, "empty"
            
            self.logger.info(f"Successfully extracted {len(extracted_text)} characters from PDF")
            return extracted_text, "success"
            
        except Exception as e:
            self.logger.error(f"Error parsing PDF: {str(e)}")
            return None, "failed"
    
    def _clean_text(self, text: str) -> str:
        """
        Clean and normalize extracted text.
        
        Args:
            text: Raw extracted text
            
        Returns:
            Cleaned text
        """
        if not text:
            return ""
        
        # Remove excessive whitespace
        lines = text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            # Strip whitespace from each line
            cleaned_line = line.strip()
            
            # Skip empty lines
            if cleaned_line:
                cleaned_lines.append(cleaned_line)
        
        # Join lines with single newlines
        cleaned_text = '\n'.join(cleaned_lines)
        
        # Remove excessive spaces between words
        import re
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text)
        
        return cleaned_text
    
    def parse_pdf_from_bytes(self, pdf_bytes: bytes) -> Tuple[Optional[str], str]:
        """
        Parse PDF from bytes and extract text content.
        
        Args:
            pdf_bytes: PDF file bytes
            
        Returns:
            Tuple of (extracted_text, status) where status is 'success', 'failed', or 'empty'
        """
        return self.parse_pdf_content(pdf_bytes)
    
    def parse_pdf_from_file(self, file_path: str) -> Tuple[Optional[str], str]:
        """
        Parse PDF from file path and extract text content.
        
        Args:
            file_path: Path to the PDF file
            
        Returns:
            Tuple of (extracted_text, status) where status is 'success', 'failed', or 'empty'
        """
        try:
            with open(file_path, 'rb') as f:
                pdf_bytes = f.read()
            return self.parse_pdf_content(pdf_bytes)
        except FileNotFoundError:
            self.logger.error(f"PDF file not found: {file_path}")
            return None, "failed"
        except Exception as e:
            self.logger.error(f"Error reading PDF file {file_path}: {str(e)}")
            return None, "failed"
    

    def get_file_size_mb(self, pdf_bytes: bytes) -> float:
        """
        Get file size in megabytes.
        
        Args:
            pdf_bytes: PDF file bytes
            
        Returns:
            File size in MB
        """
        return len(pdf_bytes) / (1024 * 1024)
