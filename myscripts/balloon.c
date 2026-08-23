#define _GNU_SOURCE
#include <numaif.h>
#include <sys/mman.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv) {
    int node = atoi(argv[1]);
    size_t bytes = strtoull(argv[2], NULL, 10) * (1UL << 30);
    void *p = mmap(NULL, bytes, PROT_READ|PROT_WRITE,
                   MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE, -1, 0);
    unsigned long mask = 1UL << node;
    mbind(p, bytes, MPOL_BIND, &mask, sizeof(mask)*8, MPOL_MF_STRICT|MPOL_MF_MOVE);
    memset(p, 1, bytes);   // touch every page → landed on target node
    mlock(p, bytes);       // pin so kernel can't demote or reclaim
    fprintf(stderr, "balloon: %zu GB on node %d (pid %d)\n", bytes>>30, node, getpid());
    pause();
    return 0;
}