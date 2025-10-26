"""
Unit tests for the PDF parser.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import io

# Mock pymupdf module before importing PDFParser
import sys
sys.modules['pymupdf'] = Mock()

from universal_search.parsers import PDFParser


class TestPDFParser:
    """Test cases for PDFParser."""
    
    def test_init(self):
        """Test PDFParser initialization."""
        parser = PDFParser()
        assert parser.logger is not None
    

    def test_get_file_size_mb(self):
        """Test file size calculation."""
        parser = PDFParser()
        
        # Test with 1MB of data
        test_bytes = b"x" * (1024 * 1024)
        size_mb = parser.get_file_size_mb(test_bytes)
        assert size_mb == 1.0
        
        # Test with 500KB of data
        test_bytes = b"x" * (500 * 1024)
        size_mb = parser.get_file_size_mb(test_bytes)
        assert abs(size_mb - 0.48828125) < 0.001  # 500KB = 0.48828125 MB
    
    def test_clean_text(self):
        """Test text cleaning functionality."""
        parser = PDFParser()
        
        # Test with normal text
        text = "This is normal text."
        cleaned = parser._clean_text(text)
        assert cleaned == "This is normal text."
        
        # Test with excessive whitespace
        text = "This   has    excessive    spaces"
        cleaned = parser._clean_text(text)
        assert cleaned == "This has excessive spaces"
        
        # Test with empty text
        text = ""
        cleaned = parser._clean_text(text)
        assert cleaned == ""
        
        # Test with None
        text = None
        cleaned = parser._clean_text(text)
        assert cleaned == ""
    
    @patch('universal_search.parsers.pdf_parser.pymupdf')
    def test_parse_pdf_content_success(self, mock_pymupdf):
        """Test successful PDF content parsing."""
        parser = PDFParser()
        
        # Mock PDF document
        mock_doc = Mock()
        mock_doc.page_count = 2
        
        mock_page1 = Mock()
        mock_page1.get_text.return_value = "Page 1 content"
        
        mock_page2 = Mock()
        mock_page2.get_text.return_value = "Page 2 content"
        
        # Use MagicMock to support __getitem__
        mock_doc.__getitem__ = Mock(side_effect=lambda x: mock_page1 if x == 0 else mock_page2)
        
        mock_pymupdf.open.return_value = mock_doc
        
        # Test parsing
        pdf_bytes = b"fake pdf content"
        text, status = parser.parse_pdf_content(pdf_bytes)
        
        assert status == "success"
        assert "Page 1 content" in text
        assert "Page 2 content" in text
        mock_doc.close.assert_called_once()
    
    @patch('universal_search.parsers.pdf_parser.pymupdf')
    def test_parse_pdf_content_empty(self, mock_pymupdf):
        """Test PDF parsing with empty content."""
        parser = PDFParser()
        
        # Mock PDF document with empty content
        mock_doc = Mock()
        mock_doc.page_count = 1
        
        mock_page = Mock()
        mock_page.get_text.return_value = "   \n\n   "  # Only whitespace
        
        mock_doc.__getitem__ = Mock(return_value=mock_page)
        mock_pymupdf.open.return_value = mock_doc
        
        # Test parsing
        pdf_bytes = b"fake pdf content"
        text, status = parser.parse_pdf_content(pdf_bytes)
        
        assert status == "empty"
        assert text is None
    
    @patch('universal_search.parsers.pdf_parser.pymupdf')
    def test_parse_pdf_content_error(self, mock_pymupdf):
        """Test PDF parsing with error."""
        parser = PDFParser()
        
        # Mock pymupdf to raise an exception
        mock_pymupdf.open.side_effect = Exception("PDF parsing error")
        
        # Test parsing
        pdf_bytes = b"fake pdf content"
        text, status = parser.parse_pdf_content(pdf_bytes)
        
        assert status == "failed"
        assert text is None
    
    @patch('universal_search.parsers.pdf_parser.pymupdf')
    def test_parse_pdf_content_pymupdf_error(self, mock_pymupdf):
        """Test PDF parsing with pymupdf-specific error."""
        parser = PDFParser()
        
        # Mock pymupdf to raise a generic Exception (simulating pymupdf error)
        # We can't use pymupdf.FileDataError since pymupdf is mocked
        class MockPymupdfError(Exception):
            pass
        
        mock_pymupdf.open.side_effect = MockPymupdfError("Invalid PDF data")
        
        # Test parsing
        pdf_bytes = b"fake pdf content"
        text, status = parser.parse_pdf_content(pdf_bytes)
        
        assert status == "failed"
        assert text is None
    
    @patch('universal_search.parsers.pdf_parser.pymupdf')
    def test_parse_pdf_from_bytes_success(self, mock_pymupdf):
        """Test PDF parsing from bytes."""
        parser = PDFParser()
        
        # Mock PDF document
        mock_doc = Mock()
        mock_doc.page_count = 1
        
        mock_page = Mock()
        mock_page.get_text.return_value = "Extracted text content"
        
        mock_doc.__getitem__ = Mock(return_value=mock_page)
        mock_pymupdf.open.return_value = mock_doc
        
        # Test parsing
        pdf_bytes = b"fake pdf content"
        text, status = parser.parse_pdf_from_bytes(pdf_bytes)
        
        assert status == "success"
        assert text == "Extracted text content"
    
    @patch('builtins.open', create=True)
    @patch('universal_search.parsers.pdf_parser.pymupdf')
    def test_parse_pdf_from_file_success(self, mock_pymupdf, mock_open):
        """Test PDF parsing from file path."""
        parser = PDFParser()
        
        # Mock file reading
        mock_file = Mock()
        mock_file.read.return_value = b"fake pdf content"
        mock_open.return_value.__enter__.return_value = mock_file
        
        # Mock PDF document
        mock_doc = Mock()
        mock_doc.page_count = 1
        
        mock_page = Mock()
        mock_page.get_text.return_value = "Extracted text content"
        
        mock_doc.__getitem__ = Mock(return_value=mock_page)
        mock_pymupdf.open.return_value = mock_doc
        
        # Test parsing
        text, status = parser.parse_pdf_from_file("/path/to/file.pdf")
        
        assert status == "success"
        assert text == "Extracted text content"
        mock_open.assert_called_once_with("/path/to/file.pdf", 'rb')
    
    @patch('builtins.open', create=True)
    def test_parse_pdf_from_file_not_found(self, mock_open):
        """Test PDF parsing with file not found."""
        parser = PDFParser()
        
        # Mock file not found error
        mock_open.side_effect = FileNotFoundError("File not found")
        
        # Test parsing
        text, status = parser.parse_pdf_from_file("/nonexistent/file.pdf")
        
        assert status == "failed"
        assert text is None
