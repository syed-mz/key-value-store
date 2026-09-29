#include "table.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Must be a power of two: bucket_of() masks instead of using %, and doubling
// keeps it a power of two forever.
#define INITIAL_CAPACITY 16

struct entry {
    char *key;
    char *value;
    struct entry *next; // next entry in the same bucket
};

struct table {
    // One lock for the whole table: every public function holds it for its
    // entire body, so operations never interleave. See README for the
    // trade-off versus finer-grained locking.
    pthread_mutex_t lock;
    struct entry **buckets;
    size_t capacity; // number of buckets
    size_t count;    // number of entries
};

// FNV-1a: tiny, fast, and spreads short similar keys ("user:1", "user:2")
// across buckets well enough for a hash table.
static uint64_t hash(const char *s)
{
    uint64_t h = 14695981039346656037ULL; // FNV offset basis
    for (; *s != '\0'; s++) {
        h ^= (unsigned char)*s;
        h *= 1099511628211ULL; // FNV prime
    }
    return h;
}

static size_t bucket_of(const char *key, size_t capacity)
{
    return (size_t)(hash(key) & (capacity - 1));
}

// Returns the pointer that points at key's entry, or at the NULL that ends
// the chain if key is absent. Returning the link itself (rather than the
// entry) lets set and del insert or unlink without tracking a "prev" pointer.
static struct entry **find_link(struct table *t, const char *key)
{
    struct entry **link = &t->buckets[bucket_of(key, t->capacity)];
    while (*link != NULL && strcmp((*link)->key, key) != 0)
        link = &(*link)->next;
    return link;
}

static void free_entry(struct entry *e)
{
    free(e->key);
    free(e->value);
    free(e);
}

// Doubling keeps chains short so lookups stay O(1) on average. A single
// rehash is O(n), but it only happens after n inserts, so inserts are still
// amortized O(1). Entries are relinked, not copied, so no key or value moves.
static void grow(struct table *t)
{
    size_t new_capacity = t->capacity * 2;
    struct entry **new_buckets = calloc(new_capacity, sizeof *new_buckets);
    if (new_buckets == NULL)
        return; // still correct, just longer chains until the next try

    for (size_t i = 0; i < t->capacity; i++) {
        struct entry *e = t->buckets[i];
        while (e != NULL) {
            struct entry *next = e->next;
            size_t b = bucket_of(e->key, new_capacity);
            e->next = new_buckets[b];
            new_buckets[b] = e;
            e = next;
        }
    }
    free(t->buckets);
    t->buckets = new_buckets;
    t->capacity = new_capacity;
}

struct table *table_create(void)
{
    struct table *t = malloc(sizeof *t);
    if (t == NULL)
        return NULL;
    t->buckets = calloc(INITIAL_CAPACITY, sizeof *t->buckets);
    if (t->buckets == NULL) {
        free(t);
        return NULL;
    }
    pthread_mutex_init(&t->lock, NULL);
    t->capacity = INITIAL_CAPACITY;
    t->count = 0;
    return t;
}

// Not locked: the caller must guarantee no other thread still uses t.
void table_destroy(struct table *t)
{
    for (size_t i = 0; i < t->capacity; i++) {
        struct entry *e = t->buckets[i];
        while (e != NULL) {
            struct entry *next = e->next;
            free_entry(e);
            e = next;
        }
    }
    pthread_mutex_destroy(&t->lock);
    free(t->buckets);
    free(t);
}

// The *_locked functions are the single-threaded logic; they assume the
// caller holds t->lock. Keeping locking in thin wrappers makes it obvious
// that every path through the table is protected.

static bool set_locked(struct table *t, const char *key, const char *value)
{
    // Allocate before touching the table so a failure leaves it unchanged.
    char *v = strdup(value);
    if (v == NULL)
        return false;

    struct entry **link = find_link(t, key);
    if (*link != NULL) {
        free((*link)->value);
        (*link)->value = v;
        return true;
    }

    struct entry *e = malloc(sizeof *e);
    char *k = strdup(key);
    if (e == NULL || k == NULL) {
        free(e);
        free(k);
        free(v);
        return false;
    }
    e->key = k;
    e->value = v;
    e->next = NULL;
    *link = e;
    t->count++;

    // Load factor > 0.75, in integer math to avoid floating point.
    if (t->count * 4 > t->capacity * 3)
        grow(t);
    return true;
}

static bool get_locked(struct table *t, const char *key, char *out,
                       size_t out_size)
{
    struct entry *e = *find_link(t, key);
    if (e == NULL)
        return false;
    snprintf(out, out_size, "%s", e->value);
    return true;
}

static bool del_locked(struct table *t, const char *key)
{
    struct entry **link = find_link(t, key);
    struct entry *e = *link;
    if (e == NULL)
        return false;
    *link = e->next;
    free_entry(e);
    t->count--;
    return true;
}

bool table_set(struct table *t, const char *key, const char *value)
{
    pthread_mutex_lock(&t->lock);
    bool ok = set_locked(t, key, value);
    pthread_mutex_unlock(&t->lock);
    return ok;
}

bool table_get(struct table *t, const char *key, char *out, size_t out_size)
{
    pthread_mutex_lock(&t->lock);
    bool found = get_locked(t, key, out, out_size);
    pthread_mutex_unlock(&t->lock);
    return found;
}

bool table_del(struct table *t, const char *key)
{
    pthread_mutex_lock(&t->lock);
    bool found = del_locked(t, key);
    pthread_mutex_unlock(&t->lock);
    return found;
}
