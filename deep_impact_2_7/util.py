#--*--coding:utf-8--*--

import zlib

ZipFileVariableLengthFileldStartPosition = 30
ZipFileExtFieldLengthFieldSize = 2
ZipFileExtFieldLengthOffset = 0x1c
ZipFileFileNameLengthFieldSize = 2
ZipFileFileNameLengthOffset = 0x1a
ZipFileFileLengthFieldSize = 4
ZipFileFileLengthOffset = 0x12
ZipFileLocalFileHeaderSignature = b"\x50\x4b\x03\x04"

#
"""
decompress_test_logic
for idx, byte_seq in enumerate(extract_zip_file_entry("C:\github\deep_impact_2_7\dataflows\gcs_at_local\inputs\#local_jrdb\\bac.zip")):
    with open("C:\github\deep_impact_2_7\dataflows\gcs_at_local\inputs\#local_jrdb\\test{num}.txt".format(num = idx),"wb") as writer:
        writer.write(byte_seq)

"""


def calc_integer_from_le(le_byte_seq):
    return sum([b << idx * 8 for idx,b in enumerate(le_byte_seq)])

def extract_zip_file_entry(byte_seq):
    #byte_seq = bytearray(open(file_name,"rb").read())
    offset = byte_seq.find(ZipFileLocalFileHeaderSignature)
    ret = []
    while offset >= 0:
        file_length_start_offset = ZipFileFileLengthOffset + offset
        file_length_length = ZipFileFileLengthFieldSize
        file_name_length_start_offset = ZipFileFileNameLengthOffset+ offset
        file_name_length_length = ZipFileFileNameLengthFieldSize
        ext_field_length_start_offset = ZipFileExtFieldLengthOffset + offset
        ext_field_length_length = ZipFileExtFieldLengthFieldSize
        file_length = calc_integer_from_le(byte_seq[file_length_start_offset:file_length_start_offset + file_length_length])
        file_name_length = calc_integer_from_le(byte_seq[file_name_length_start_offset:file_name_length_start_offset + file_name_length_length])
        ext_field_length = calc_integer_from_le(byte_seq[ext_field_length_start_offset:ext_field_length_start_offset + ext_field_length_length])
        #ファイル名長さ:10+a,10+b
        #拡張フィールド長さ:10+c,10+d
        content_start_position = offset + ZipFileVariableLengthFileldStartPosition + file_name_length + ext_field_length
        content = byte_seq[content_start_position:content_start_position + file_length]
        file_name_start_position = offset + ZipFileVariableLengthFileldStartPosition
        file_name = byte_seq[file_name_start_position : file_name_start_position + file_name_length]
        debug("file {file_name} is detected decompressing...".format(file_name = file_name))
        ret.append((str(file_name), zlib.decompress(str(content),-zlib.MAX_WBITS)))
        debug("complete decompressing")
        offset = byte_seq.find(ZipFileLocalFileHeaderSignature, offset + 1) #offset1とかにすると一つしかファイルなければ-1
    return ret


def debug(str):
    pass
    #print str
def alert(str):
    pass
    #print str

def info(str):
    pass
    print str
