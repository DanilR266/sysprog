#include "userfs.h"
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

enum {
	BLOCK_SIZE = 512,
	MAX_FILE_SIZE = 1024 * 1024 * 100,
};

/** Global error code. Set from any function on any error. */
static enum ufs_error_code ufs_error_code = UFS_ERR_NO_ERR;

struct block {
	/** Block memory. */
	char *memory;
	/** How many bytes are occupied. */
	size_t occupied;
	/** Next block in the file. */
	struct block *next;
	/** Previous block in the file. */
	struct block *prev;

	/* PUT HERE OTHER MEMBERS */
};

struct file {
	/** Double-linked list of file blocks. */
	struct block *block_list;
	/**
	 * Last block in the list above for fast access to the end
	 * of file.
	 */
	struct block *last_block;
	/** How many file descriptors are opened on the file. */
	int refs;
	/** File name. */
	char *name;
	/** Files are stored in a double-linked list. */
	struct file *next;
	struct file *prev;
	/* PUT HERE OTHER MEMBERS */
    bool is_deleted;
    size_t size;
};

/** List of all files. */
static struct file *file_list = NULL;

struct filedesc {
	struct file *file;
	/* PUT HERE OTHER MEMBERS */
    size_t pos;
    bool is_open;
};

/**
 * An array of file descriptors. When a file descriptor is
 * created, its pointer drops here. When a file descriptor is
 * closed, its place in this array is set to NULL and can be
 * taken by next ufs_open() call.
 */
static struct filedesc **file_descriptors = NULL;
static int file_descriptor_count = 0;
static int file_descriptor_capacity = 0;

enum ufs_error_code
ufs_errno()
{
	return ufs_error_code;
}

struct file
*find_file(const char *filename) {
    struct file *f = file_list;
    while (f != NULL) {
        if (strcmp(f->name, filename) == 0 && !f->is_deleted) {
            return f;
        }
        f = f->next;
    }
    return NULL;
}

struct file
*create_file(const char *filename) {
    struct file *f = malloc(sizeof(struct file));
    f->name = strdup(filename);
    f->block_list = NULL;
    f->last_block = NULL;
    f->refs = 0;
    f->is_deleted = false;
    f->size = 0;
    f->next = file_list;
    f->prev = NULL;
    if (file_list != NULL) {
        file_list->prev = f;
    }
    file_list = f;
    return f;
}

struct block
*create_block() {
    struct block *b = malloc(sizeof(struct block));
    b->memory = malloc(BLOCK_SIZE);
    b->occupied = 0;
    b->next = NULL;
    b->prev = NULL;
    return b;
}

void
append_block(struct file *f) {
    if (f->size >= MAX_FILE_SIZE) {
        ufs_error_code = UFS_ERR_NO_MEM;
        return;
    }
    struct block *b = create_block();
    if (f->last_block == NULL) {
        f->block_list = b;
    } else {
        f->last_block->next = b;
        b->prev = f->last_block;
    }
    f->last_block = b;
}

int
ufs_open(const char *filename, int flags) {
    struct file *f = find_file(filename);
    
    if (f != NULL && f->is_deleted) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    if (f == NULL && !(flags & UFS_CREATE)) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    if (f == NULL) {
        f = create_file(filename);
    }

    struct filedesc *fd = malloc(sizeof(struct filedesc));
    fd->file = f;
    fd->pos = 0;
    fd->is_open = true;
    f->refs++;

    if (file_descriptor_count == file_descriptor_capacity) {
        file_descriptor_capacity = (file_descriptor_capacity == 0) ? 1 : file_descriptor_capacity * 2;
        file_descriptors = realloc(file_descriptors, file_descriptor_capacity * sizeof(struct filedesc *));
    }
    file_descriptors[file_descriptor_count++] = fd;

    return file_descriptor_count - 1;
}

ssize_t
ufs_write(int fd, const char *buf, size_t size) {
    if (fd < 0 || fd >= file_descriptor_count || file_descriptors[fd] == NULL || !file_descriptors[fd]->is_open) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    struct filedesc *desc = file_descriptors[fd];
    struct file *f = desc->file;
    
    if (f->size >= MAX_FILE_SIZE && desc->pos + size > f->size) {
        ufs_error_code = UFS_ERR_NO_MEM;
        return -1;
    }

    if (desc->pos + size > f->size && f->size + (desc->pos + size - f->size) > MAX_FILE_SIZE) {
        size = MAX_FILE_SIZE - desc->pos;
        if (size == 0) {
            ufs_error_code = UFS_ERR_NO_MEM;
            return -1;
        }
    }
    
    size_t written = 0;

    while (written < size) {
        int block_num = desc->pos / BLOCK_SIZE;
        size_t offset = desc->pos % BLOCK_SIZE;

        struct block *b = f->block_list;
        for (int i = 0; i < block_num && b != NULL; i++) {
            b = b->next;
        }

        if (b == NULL) {
            append_block(f);
            b = f->last_block;
        }

        size_t space_in_block = BLOCK_SIZE - offset;
        size_t to_write = (size - written) < space_in_block ? (size - written) : space_in_block;

        memcpy(b->memory + offset, buf + written, to_write);
        written += to_write;
        desc->pos += to_write;
        
        if (offset + to_write > b->occupied) {
            b->occupied = offset + to_write;
        }
        if (desc->pos > f->size) {
            f->size = desc->pos;
        }
    }

    return written;
}

ssize_t
ufs_read(int fd, char *buf, size_t size) {
    if (fd < 0 || fd >= file_descriptor_count || file_descriptors[fd] == NULL || !file_descriptors[fd]->is_open) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    struct filedesc *desc = file_descriptors[fd];
    struct file *f = desc->file;
    size_t read_bytes = 0;

    while (read_bytes < size && desc->pos < f->size) {
        int block_num = desc->pos / BLOCK_SIZE;
        int offset = desc->pos % BLOCK_SIZE;

        struct block *b = f->block_list;
        for (int i = 0; i < block_num && b != NULL; i++) {
            b = b->next;
        }

        if (b == NULL) break;

        size_t available_in_block = b->occupied - offset;
        size_t to_read = (size - read_bytes) < available_in_block ? (size - read_bytes) : available_in_block;

        memcpy(buf + read_bytes, b->memory + offset, to_read);
        read_bytes += to_read;
        desc->pos += to_read;
    }

    return read_bytes;
}

int
ufs_close(int fd) {
    if (fd < 0 || fd >= file_descriptor_count || file_descriptors[fd] == NULL || !file_descriptors[fd]->is_open) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    struct filedesc *desc = file_descriptors[fd];
    struct file *f = desc->file;

    f->refs--;
    if (f->refs == 0 && f->is_deleted) {
        if (f->prev != NULL) {
            f->prev->next = f->next;
        } else {
            file_list = f->next;
        }
        
        if (f->next != NULL) {
            f->next->prev = f->prev;
        }
        struct block *b = f->block_list;
        while (b != NULL) {
            struct block *next = b->next;
            free(b->memory);
            free(b);
            b = next;
        }
        free(f->name);
        free(f);
    }
    
    free(desc);
    file_descriptors[fd] = NULL;
    return 0;
}

int
ufs_delete(const char *filename) {
    struct file *f = find_file(filename);
    if (f == NULL) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    f->is_deleted = true;
    if (f->refs == 0) {
        if (f->prev) {
            f->prev->next = f->next;
        }
        if (f->next) {
            f->next->prev = f->prev;
        }
        if (file_list == f) {
            file_list = f->next;
        }

        struct block *b = f->block_list;
        while (b != NULL) {
            struct block *next = b->next;
            free(b->memory);
            free(b);
            b = next;
        }
        
        free(f->name);
        free(f);
    }

    return 0;
}

#if NEED_RESIZE

int
ufs_resize(int fd, size_t new_size)
{
	/* IMPLEMENT THIS FUNCTION */
	(void)fd;
	(void)new_size;
	ufs_error_code = UFS_ERR_NOT_IMPLEMENTED;
	return -1;
}

#endif

void
ufs_destroy(void) {
    struct file *f = file_list;
    while (f != NULL) {
        struct file *next = f->next;
        struct block *b = f->block_list;
        while (b != NULL) {
            struct block *next_block = b->next;
            free(b->memory);
            free(b);
            b = next_block;
        }
        free(f->name);
        free(f);
        f = next;
    }
    file_list = NULL;
    for (int i = 0; i < file_descriptor_count; i++) {
        if (file_descriptors[i] != NULL) {
            free(file_descriptors[i]);
        }
    }
    free(file_descriptors);
    file_descriptors = NULL;
    file_descriptor_count = 0;
    file_descriptor_capacity = 0;
}
