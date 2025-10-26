"""
Text chunker component for splitting text into overlapping chunks.

This module provides functionality to split text into chunks with configurable
window size and overlap, while preserving text boundaries where possible.
"""

import re
import logging
from typing import List, Dict, Any
from dataclasses import dataclass


@dataclass
class TextChunk:
    """Represents a chunk of text with metadata."""
    
    chunk_id: str
    chunk_index: int
    text: str
    start_position: int
    end_position: int
    total_chunks: int


class TextChunker:
    """Text chunker for splitting text into overlapping chunks."""
    
    def __init__(self, window_size: int = 1000, overlap: int = 200):
        """
        Initialize the text chunker.
        
        Args:
            window_size: Size of each chunk in characters
            overlap: Number of characters to overlap between chunks
        """
        self.window_size = window_size
        self.overlap = overlap
        self.logger = logging.getLogger(__name__)
        
        # Validate parameters
        if overlap >= window_size:
            raise ValueError("Overlap must be less than window size")
        if window_size <= 0 or overlap < 0:
            raise ValueError("Window size must be positive and overlap must be non-negative")
    
    def chunk_text(self, text: str, file_id: str) -> List[TextChunk]:
        """
        Split text into overlapping chunks.
        
        Args:
            text: Text to chunk
            file_id: File identifier for generating chunk IDs
            
        Returns:
            List of TextChunk objects
        """
        if not text or not text.strip():
            self.logger.warning(f"No text to chunk for file {file_id}")
            return []
        
        # Clean and normalize text
        cleaned_text = self._clean_text(text)
        
        if len(cleaned_text) <= self.window_size:
            # Text fits in single chunk
            chunk = TextChunk(
                chunk_id=f"{file_id}_chunk_0",
                chunk_index=0,
                text=cleaned_text,
                start_position=0,
                end_position=len(cleaned_text),
                total_chunks=1
            )
            return [chunk]
        
        chunks = []
        start_pos = 0
        chunk_index = 0
        
        while start_pos < len(cleaned_text):
            # Calculate end position for this chunk
            end_pos = min(start_pos + self.window_size, len(cleaned_text))
            
            # Extract chunk text
            chunk_text = cleaned_text[start_pos:end_pos]
            
            # Try to break at sentence boundary if possible
            if end_pos < len(cleaned_text):
                chunk_text, actual_end_pos = self._break_at_boundary(
                    cleaned_text, start_pos, end_pos
                )
                end_pos = actual_end_pos
            
            # Create chunk
            chunk = TextChunk(
                chunk_id=f"{file_id}_chunk_{chunk_index}",
                chunk_index=chunk_index,
                text=chunk_text,
                start_position=start_pos,
                end_position=end_pos,
                total_chunks=0  # Will be updated later
            )
            chunks.append(chunk)
            
            # Move to next chunk position
            start_pos = end_pos - self.overlap
            chunk_index += 1
            
            # Prevent infinite loop
            if start_pos <= chunks[-1].start_position:
                start_pos = chunks[-1].end_position
        
        # Update total chunks count
        for chunk in chunks:
            chunk.total_chunks = len(chunks)
        
        self.logger.info(f"Created {len(chunks)} chunks for file {file_id}")
        return chunks
    
    def _clean_text(self, text: str) -> str:
        """
        Clean and normalize text for chunking.
        
        Args:
            text: Raw text
            
        Returns:
            Cleaned text
        """
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove control characters except newlines
        text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
        
        return text.strip()
    
    def _break_at_boundary(self, text: str, start_pos: int, end_pos: int) -> tuple[str, int]:
        """
        Try to break chunk at sentence or word boundary.
        
        Args:
            text: Full text
            start_pos: Start position of chunk
            end_pos: Desired end position of chunk
            
        Returns:
            Tuple of (chunk_text, actual_end_position)
        """
        chunk_text = text[start_pos:end_pos]
        
        # Look for sentence endings within the last 100 characters
        search_start = max(0, len(chunk_text) - 100)
        sentence_endings = ['.', '!', '?', '\n']
        
        for i in range(len(chunk_text) - 1, search_start - 1, -1):
            if chunk_text[i] in sentence_endings:
                # Found a sentence boundary
                actual_end_pos = start_pos + i + 1
                return text[start_pos:actual_end_pos], actual_end_pos
        
        # Look for word boundaries within the last 50 characters
        search_start = max(0, len(chunk_text) - 50)
        for i in range(len(chunk_text) - 1, search_start - 1, -1):
            if chunk_text[i] == ' ':
                # Found a word boundary
                actual_end_pos = start_pos + i
                return text[start_pos:actual_end_pos], actual_end_pos
        
        # No good boundary found, use original end position
        return chunk_text, end_pos
    
    def chunk_text_simple(self, text: str, file_id: str) -> List[TextChunk]:
        """
        Simple chunking without boundary preservation (fallback method).
        
        Args:
            text: Text to chunk
            file_id: File identifier for generating chunk IDs
            
        Returns:
            List of TextChunk objects
        """
        if not text or not text.strip():
            return []
        
        cleaned_text = self._clean_text(text)
        
        if len(cleaned_text) <= self.window_size:
            chunk = TextChunk(
                chunk_id=f"{file_id}_chunk_0",
                chunk_index=0,
                text=cleaned_text,
                start_position=0,
                end_position=len(cleaned_text),
                total_chunks=1
            )
            return [chunk]
        
        chunks = []
        chunk_index = 0
        
        for i in range(0, len(cleaned_text), self.window_size - self.overlap):
            end_pos = min(i + self.window_size, len(cleaned_text))
            chunk_text = cleaned_text[i:end_pos]
            
            chunk = TextChunk(
                chunk_id=f"{file_id}_chunk_{chunk_index}",
                chunk_index=chunk_index,
                text=chunk_text,
                start_position=i,
                end_position=end_pos,
                total_chunks=0
            )
            chunks.append(chunk)
            chunk_index += 1
        
        # Update total chunks count
        for chunk in chunks:
            chunk.total_chunks = len(chunks)
        
        return chunks
    
    def get_chunk_statistics(self, chunks: List[TextChunk]) -> Dict[str, Any]:
        """
        Get statistics about the chunks.
        
        Args:
            chunks: List of TextChunk objects
            
        Returns:
            Dictionary with chunk statistics
        """
        if not chunks:
            return {
                'total_chunks': 0,
                'total_characters': 0,
                'average_chunk_size': 0,
                'min_chunk_size': 0,
                'max_chunk_size': 0
            }
        
        chunk_sizes = [len(chunk.text) for chunk in chunks]
        total_chars = sum(chunk_sizes)
        
        return {
            'total_chunks': len(chunks),
            'total_characters': total_chars,
            'average_chunk_size': total_chars / len(chunks),
            'min_chunk_size': min(chunk_sizes),
            'max_chunk_size': max(chunk_sizes)
        }
