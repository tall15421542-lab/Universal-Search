"""
Unit tests for the text chunker.
"""

import pytest

from universal_search.chunkers import TextChunker, TextChunk


class TestTextChunker:
    """Test cases for TextChunker."""
    
    def test_init(self):
        """Test TextChunker initialization."""
        chunker = TextChunker(1000, 200)
        assert chunker.window_size == 1000
        assert chunker.overlap == 200
    
    def test_init_invalid_params(self):
        """Test TextChunker initialization with invalid parameters."""
        with pytest.raises(ValueError, match="Overlap must be less than window size"):
            TextChunker(100, 200)
        
        with pytest.raises(ValueError, match="Overlap must be less than window size"):
            TextChunker(0, 100)
        
        with pytest.raises(ValueError, match="overlap must be non-negative"):
            TextChunker(1000, -100)
    
    def test_chunk_text_empty(self):
        """Test chunking empty text."""
        chunker = TextChunker(1000, 200)
        chunks = chunker.chunk_text("", "test_file")
        assert chunks == []
    
    def test_chunk_text_none(self):
        """Test chunking None text."""
        chunker = TextChunker(1000, 200)
        chunks = chunker.chunk_text(None, "test_file")
        assert chunks == []
    
    def test_chunk_text_single_chunk(self):
        """Test chunking text that fits in single chunk."""
        chunker = TextChunker(1000, 200)
        text = "This is a short text that fits in one chunk."
        
        chunks = chunker.chunk_text(text, "test_file")
        
        assert len(chunks) == 1
        assert chunks[0].chunk_id == "test_file_chunk_0"
        assert chunks[0].chunk_index == 0
        assert chunks[0].text == text
        assert chunks[0].start_position == 0
        assert chunks[0].end_position == len(text)
        assert chunks[0].total_chunks == 1
    
    def test_chunk_text_multiple_chunks(self):
        """Test chunking text that requires multiple chunks."""
        chunker = TextChunker(50, 10)  # Small chunks for testing
        text = "This is a longer text that will be split into multiple chunks for testing purposes."
        
        chunks = chunker.chunk_text(text, "test_file")
        
        assert len(chunks) > 1
        
        # Check first chunk
        assert chunks[0].chunk_id == "test_file_chunk_0"
        assert chunks[0].chunk_index == 0
        assert chunks[0].start_position == 0
        assert chunks[0].total_chunks == len(chunks)
        
        # Check last chunk
        last_chunk = chunks[-1]
        assert last_chunk.chunk_index == len(chunks) - 1
        assert last_chunk.end_position == len(text)
        
        # Check overlap between chunks
        for i in range(1, len(chunks)):
            current_chunk = chunks[i]
            previous_chunk = chunks[i - 1]
            
            # Current chunk should start before previous chunk ends (overlap)
            assert current_chunk.start_position < previous_chunk.end_position
    
    def test_chunk_text_simple(self):
        """Test simple chunking without boundary preservation."""
        chunker = TextChunker(50, 10)
        text = "This is a test text for simple chunking without boundary preservation."
        
        chunks = chunker.chunk_text_simple(text, "test_file")
        
        assert len(chunks) > 1
        
        # Check that chunks are properly spaced
        for i in range(1, len(chunks)):
            current_chunk = chunks[i]
            previous_chunk = chunks[i - 1]
            
            # Current chunk should start at previous chunk end minus overlap
            expected_start = previous_chunk.end_position - chunker.overlap
            assert current_chunk.start_position == expected_start
    
    def test_chunk_text_simple_empty(self):
        """Test simple chunking with empty text."""
        chunker = TextChunker(50, 10)
        chunks = chunker.chunk_text_simple("", "test_file")
        assert chunks == []
    
    def test_chunk_text_simple_single_chunk(self):
        """Test simple chunking with text that fits in single chunk."""
        chunker = TextChunker(100, 20)
        text = "This is a short text."
        
        chunks = chunker.chunk_text_simple(text, "test_file")
        
        assert len(chunks) == 1
        assert chunks[0].chunk_id == "test_file_chunk_0"
        assert chunks[0].chunk_index == 0
        assert chunks[0].total_chunks == 1
    
    def test_clean_text(self):
        """Test text cleaning functionality."""
        chunker = TextChunker(1000, 200)
        
        # Test with normal text
        text = "This is normal text."
        cleaned = chunker._clean_text(text)
        assert cleaned == "This is normal text."
        
        # Test with excessive whitespace
        text = "This   has    excessive    spaces"
        cleaned = chunker._clean_text(text)
        assert cleaned == "This has excessive spaces"
        
        # Test with control characters
        text = "Text with\x00control\x01characters"
        cleaned = chunker._clean_text(text)
        assert "\x00" not in cleaned
        assert "\x01" not in cleaned
        
        # Test with empty text
        text = ""
        cleaned = chunker._clean_text(text)
        assert cleaned == ""
    
    def test_break_at_boundary(self):
        """Test boundary breaking functionality."""
        chunker = TextChunker(1000, 200)
        
        text = "This is a sentence. This is another sentence! And a third one?"
        
        # Test breaking at sentence boundary
        chunk_text, end_pos = chunker._break_at_boundary(text, 0, 30)
        assert chunk_text == "This is a sentence."
        assert end_pos == 19  # Position after the period
        
        # Test breaking at word boundary
        chunk_text, end_pos = chunker._break_at_boundary(text, 0, 15)
        assert chunk_text == "This is a"
        assert end_pos == 9  # Position after "a"
        
        # Test no good boundary found
        chunk_text, end_pos = chunker._break_at_boundary(text, 0, 5)
        assert chunk_text == "This"  # No space at the end
        assert end_pos == 4  # Position after "s"
    
    def test_break_at_boundary_no_boundary_found(self):
        """Test boundary breaking when no boundary is found (uses fallback)."""
        chunker = TextChunker(1000, 200)
        
        # Text with no sentence/word boundaries in search area
        text = "NoBoundaryHere"
        chunk_text, end_pos = chunker._break_at_boundary(text, 0, 5)
        
        # Should return chunk up to end_pos without adjusting
        assert chunk_text == "NoBou"
        assert end_pos == 5
    
    def test_get_chunk_statistics(self):
        """Test chunk statistics calculation."""
        chunker = TextChunker(1000, 200)
        
        # Test with empty chunks
        stats = chunker.get_chunk_statistics([])
        assert stats['total_chunks'] == 0
        assert stats['total_characters'] == 0
        assert stats['average_chunk_size'] == 0
        
        # Test with chunks
        chunks = [
            TextChunk("id1", 0, "short", 0, 5, 3),
            TextChunk("id2", 1, "medium length", 5, 17, 3),
            TextChunk("id3", 2, "very long chunk of text", 17, 40, 3)
        ]
        
        stats = chunker.get_chunk_statistics(chunks)
        assert stats['total_chunks'] == 3
        assert stats['total_characters'] == 41  # "short" (5) + "medium length" (13) + "very long chunk of text" (23) = 41
        assert stats['average_chunk_size'] == 41 / 3
        assert stats['min_chunk_size'] == 5
        assert stats['max_chunk_size'] == 23


class TestTextChunk:
    """Test cases for TextChunk dataclass."""
    
    def test_text_chunk_creation(self):
        """Test TextChunk creation."""
        chunk = TextChunk(
            chunk_id="test_chunk_0",
            chunk_index=0,
            text="Test content",
            start_position=0,
            end_position=11,
            total_chunks=1
        )
        
        assert chunk.chunk_id == "test_chunk_0"
        assert chunk.chunk_index == 0
        assert chunk.text == "Test content"
        assert chunk.start_position == 0
        assert chunk.end_position == 11
        assert chunk.total_chunks == 1
