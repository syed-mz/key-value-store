#ifndef TABLE_H
#define TABLE_H

#include <stdbool.h>
#include <stddef.h>

// A string-to-string hash map. Keys and values are copied on insert, so
// callers are free to reuse their buffers afterwards.
struct table;

struct table *table_create(void);
void table_destroy(struct table *t);

// Inserts a new key or overwrites an existing one. Returns false only if
// memory runs out, in which case the table is unchanged.
bool table_set(struct table *t, const char *key, const char *value);

// Copies the value into out, truncated to out_size - 1 bytes. Copying instead
// of returning a pointer means the caller never holds memory that a later SET
// or DEL could free out from under it.
bool table_get(struct table *t, const char *key, char *out, size_t out_size);

// Returns false if the key was not present.
bool table_del(struct table *t, const char *key);

#endif
