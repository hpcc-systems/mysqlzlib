IMPORT Std;

EXPORT MySqlZlib := MODULE


  EXPORT Bundle := MODULE(Std.BundleBase)
    EXPORT Name := 'MysqlZlib';
    EXPORT Description := 'Mysql compatible zlib compress/decompression';
    EXPORT Authors := ['Shamser Ahmed','Charles Kaminski'];
    EXPORT License := 'http://www.apache.org/licenses/LICENSE-2.0';
    EXPORT Copyright := 'Copyright (C) 2013 HPCC Systems';
    EXPORT DependsOn := [];
    EXPORT Version := '1.0.0';
    EXPORT PlatformVersion := '4.0.0';
  END;

  /**
   * Decompresses mysql compressed zlib format blob
   * 
   * @param str1    the compressed mysql blob
   * @return        uncompressed blob or empty if str1 cannot be uncompresed
   **/
  EXPORT STRING Decompress(STRING str1) := BEGINC++
    #option pure
    #include <zlib.h>
    #include <stdio.h>
    #include "string.h"
    #include <stdlib.h>

    // Error values
    // Left for internal documentation
    /*
      #define Z_OK            0
      #define Z_STREAM_END    1
      #define Z_NEED_DICT     2
      #define Z_ERRNO        (-1)
      #define Z_STREAM_ERROR (-2)
      #define Z_DATA_ERROR   (-3)
      #define Z_MEM_ERROR    (-4)
      #define Z_BUF_ERROR    (-5)
      #define Z_VERSION_ERROR (-6)
    */

    #body		
    int err;
    uLong uncompLen;
    Byte *comp; 
    Byte *uncomp;

    // If blank, then return nothing
    if (lenStr1 == 0)
    {
      __lenResult = 0;
      return;
    }
    comp = (Byte *) str1;

    // MySQL prepends the first 4 bytes with the length of the uncompressed string

    uncompLen = (uLong) (comp[3]<<24) + (comp[2]<<16) + (comp[1]<<8) + (comp[0]);
    const char* charPayload = str1 + 4;
    uncomp = (Byte*)rtlMalloc(uncompLen);
    if ( !uncomp )
    {
        rtlFail(-1,"MySqlZlib.uncompress failed: unable to allocate memory");
    }
    else
    {
        err = uncompress(uncomp, &uncompLen, (const Bytef*)charPayload, lenStr1-4);
        if ( Z_OK==err )
        {
            __lenResult = uncompLen;
            __result    = (char *)uncomp;
            return;
        }
        rtlFree(uncomp);
    }
    __lenResult = 0;
  ENDC++;

  /**
   * Compresses string into mysql compressed zlib format blob
   * 
   * @param str1    the string to be compressed
   * @return        the compressed blob
   **/
  EXPORT STRING Compress(STRING str1) := BEGINC++
    #option pure
    #include <zlib.h>
    #include <stdio.h>
    #include "string.h"
    #include <stdlib.h>
    typedef unsigned char byte;

    #body		
    int err;
    byte *buffer, *compBlob;
    uLong compLen;
    compLen = lenStr1+lenStr1/1000+13; // size required by compress
    buffer = (byte *) rtlMalloc(compLen+4);
    if (!buffer) 
    {
        rtlFail(-1,"MySqlZlib.compress failed: unable to allocate memory");
        __lenResult = 0;
        return;
    }

    compBlob = buffer+4;
    err = compress(compBlob, &compLen, (const Bytef *)str1, (uLong) lenStr1);
    if ( Z_OK==err ) 
    {
        __lenResult = compLen+4;
        buffer[3] = ((unsigned)lenStr1 >> 24) & 0xff;
        buffer[2] = ((unsigned)lenStr1 >> 16) & 0xff;
        buffer[1] = ((unsigned)lenStr1 >> 8) & 0xff;
        buffer[0] = (unsigned)lenStr1 & 0xff;
	__result    = (char *)buffer;
	return;
    }
    else
    {
        rtlFree(buffer);
        rtlFail(err,"MySqlZlib.compress failed to compress");
        __lenResult = 0;
        return;
    }
  ENDC++;
	
END;
