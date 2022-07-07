#ifdef STANDALONE
#include <stdio.h>
#endif // STANDALONE

#include "sequences.h"

static const uint64_t a = 1664525;
static const uint64_t c = 1013904223;

uint64_t
fillbytes(uint8_t *buf, size_t size, uint64_t seed)
{
    uint64_t next = seed;
    size_t i = 0;

    // Fill in buf 8 bytes at a time for as many bytes as possible.
    if (size > (sizeof(uint64_t) + 1)) {
        for (; i < size - sizeof(uint64_t) + 1; i += sizeof(uint64_t)) {
            next = next * a + c;
        
            *((uint64_t*) buf) = next;
            buf += sizeof(uint64_t);
        }
    }
    
    // Fill in remainder (if any) single byte at a time.
    for (; i < size; i++) {
        next = next * a + c;

        *buf++ = (uint8_t) next;
    }

    return next;
}

uint64_t
fillletters(uint8_t *buf, size_t size, uint64_t seed)
{
    uint64_t next = seed;
    size_t i = 0;
    
    // Fill in buf 12 letters at a time. Since we're only using
    // lowercase letters, but generating 64 bits of randomness at a
    // time, we can use 5 bits for each letter. We can't do this at
    // the end of the buffer, but we'll finish that off below.
    // Algorithm and constants borrowed from Numerical Recipes in
    // C (2nd ed), section 7.1.
    if (size > 11) {
        for (; i < size - 11; i += 12) {
            next = next * a + c;

            *buf++ = ((next & 0x00000000000001f) >> 00) % ('z' - 'a' + 1) + 'a';
            *buf++ = ((next & 0x0000000000003e0) >> 05) % ('z' - 'a' + 1) + 'a';
            *buf++ = ((next & 0x000000000007c00) >> 10) % ('z' - 'a' + 1) + 'a';
            *buf++ = ((next & 0x0000000000f8000) >> 15) % ('z' - 'a' + 1) + 'a';
            *buf++ = ((next & 0x000000001f00000) >> 20) % ('z' - 'a' + 1) + 'a';
            *buf++ = ((next & 0x00000003e000000) >> 25) % ('z' - 'a' + 1) + 'a';
            *buf++ = ((next & 0x0000007c0000000) >> 30) % ('z' - 'a' + 1) + 'a';
            *buf++ = ((next & 0x00000f800000000) >> 35) % ('z' - 'a' + 1) + 'a';
            *buf++ = ((next & 0x0001f0000000000) >> 40) % ('z' - 'a' + 1) + 'a';
            *buf++ = ((next & 0x003e00000000000) >> 45) % ('z' - 'a' + 1) + 'a';
            *buf++ = ((next & 0x07c000000000000) >> 50) % ('z' - 'a' + 1) + 'a';
            *buf++ = ((next & 0xf80000000000000) >> 55) % ('z' - 'a' + 1) + 'a';
        }
    }

    // Fill remainder in less-efficient manner
    for (; i < size; i++) {
        next = next * a + c;

        *buf++ = next % ('z' - 'a' + 1) + 'a';
    }

    return next;
}

#ifdef STANDALONE
int
main() {
    size_t size = 32;
    uint64_t seed = 0x490c734ad1ccf6e9;
    uint8_t buf[size];

    printf("letter sequence:\n");
    
    for (int i = 0; i < 10; i++) {
        seed = fillletters(buf, size, seed);        
        printf("  %s\n", buf);
    }
    
    printf("byte sequences:\n");
    
    for (int i = 0; i < 10; i++) {
        seed = fillbytes(buf, size, seed);
        
        for (int j = 0; j < size; j++) {
            printf("%02x", buf[j]);
        }
        printf("\n");
    }
}
#endif // STANDALONE
