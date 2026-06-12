using System.Text.RegularExpressions;
using FluentAssertions;
using MSSP.Storage;

namespace MSSP.Engine.Storage;

public class SstFileInfoTests {
    [Fact]
    public void Parse_FileWithLevelSuffix_ReturnsCorrectLevel() {
        var info = SstFileInfo.Parse("/path/to/file_L2.sst", 1000);
        info.FilePath.Should().Be("/path/to/file_L2.sst");
        info.Level.Should().Be(2);
        info.SizeBytes.Should().Be(1000);
    }

    [Fact]
    public void Parse_FileWithoutLevelSuffix_DefaultsToL1() {
        var info = SstFileInfo.Parse("/path/to/file.sst", 500);
        info.FilePath.Should().Be("/path/to/file.sst");
        info.Level.Should().Be(1);
        info.SizeBytes.Should().Be(500);
    }

    [Fact]
    public void Parse_FileWithL1Suffix_ReturnsLevel1() {
        var info = SstFileInfo.Parse("/path/to/file_L1.sst", 200);
        info.Level.Should().Be(1);
    }

    [Fact]
    public void Parse_FileWithL10Suffix_ReturnsLevel10() {
        var info = SstFileInfo.Parse("/path/to/file_L10.sst", 5000);
        info.Level.Should().Be(10);
    }

    [Fact]
    public void Parse_FileWithNonNumericSuffix_DefaultsToL1() {
        var info = SstFileInfo.Parse("/path/to/file_LX.sst", 300);
        info.Level.Should().Be(1);
    }

    [Fact]
    public void BloomFilterPath_ReturnsCorrectPath() {
        var info = new SstFileInfo("file_L2.sst", 2, 100);
        info.BloomFilterPath.Should().Be("file_L2.sst.bf");
    }

    [Fact]
    public void BloomFilterPath_PreservesDirectory() {
        var info = new SstFileInfo("/data/files_L3.sst", 3, 200);
        info.BloomFilterPath.Should().Be("/data/files_L3.sst.bf");
    }

    [Fact]
    public void RecordStruct_Immutable() {
        var info = new SstFileInfo("test_L1.sst", 1, 100);
        // Record structs are immutable - just verify we can create and read them
        info.FilePath.Should().Be("test_L1.sst");
        info.Level.Should().Be(1);
        info.SizeBytes.Should().Be(100);
    }
}
