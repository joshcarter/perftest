#ifndef SEQUENCES_H_
#define SEQUENCES_H_

#include <stdint.h>
#include <stdlib.h>

uint64_t fillbytes(uint8_t *buf, size_t size, uint64_t seed);
uint64_t fillletters(uint8_t *buf, size_t size, uint64_t seed);

#endif /* SEQUENCES_H_ */


