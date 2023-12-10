#define FUSE_USE_VERSION 30
#include <fuse.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/mman.h>
#include "wfs.h"
#include "time.h"
#include <stdbool.h>

FILE *disk = NULL;
const char *disk_path;
void *mapped_disk = NULL;
size_t mapped_size;

// helper to copy inode, under desired condition
void copy_data(struct wfs_inode *newInode, struct wfs_inode *fileInode, int condition)
{

    memcpy(newInode, fileInode, sizeof(struct wfs_inode));

    // edit fields specifically as needed
    newInode->deleted = 0;
    newInode->atime = time(NULL);
    newInode->ctime = time(NULL);
    newInode->mtime = time(NULL);

    // edit size
    switch (condition)
    {
    // 0: write
    case 0:
        return;

    // 1: mkdir
    case 1:
        newInode->size = fileInode->size + sizeof(struct wfs_dentry);
        return;

    // 2: unlink
    case 2:
        newInode->size = fileInode->size - sizeof(struct wfs_dentry);
        return;
    }
}

// get a log entry by inode_number; get root entry by inode_number = 0
struct wfs_log_entry *get_log_entry(int inode_number)
{
    char *curr_addr = (char *)mapped_disk + sizeof(struct wfs_sb);
    struct wfs_sb *sb = (struct wfs_sb *)mapped_disk;
    struct wfs_log_entry *target_node = NULL;
    while (curr_addr < ((char *)mapped_disk + sb->head))
    {
        struct wfs_log_entry *curr_log_entry = (struct wfs_log_entry *)curr_addr;

        if (curr_log_entry->inode.inode_number == inode_number)
        {
            target_node = curr_log_entry;
        }

        curr_addr += sizeof(struct wfs_inode) + curr_log_entry->inode.size;
    }

    return target_node;
}

struct wfs_dentry *get_dentry(struct wfs_log_entry *entry, const char *name)
{
    int numDE = entry->inode.size / sizeof(struct wfs_dentry);
    if (numDE == 0)
    {
        return NULL;
    }
    struct wfs_dentry *currDentry = (struct wfs_dentry *)entry->data;

    // Iterate over each directory entry
    for (int i = 0; i < numDE; i++)
    {
        if (strcmp(currDentry->name, name) == 0)
        {
            return currDentry;
        }
        currDentry++;
    }
    return NULL;
}

struct wfs_inode *get_inode_by_path(const char *path)
{

    struct wfs_log_entry *logEntry = get_log_entry(0);
    char *copied_path = strdup(path);
    char *token = strtok(copied_path, "/");

    while (token)
    {
        int numDE = logEntry->inode.size / sizeof(struct wfs_dentry);
        if (numDE == 0)
            return NULL;

        struct wfs_dentry *currDentry = get_dentry(logEntry, token);
        if (currDentry == NULL)
            return NULL;

        token = strtok(NULL, "/");

        // If it was the last token in the path, return the corresponding inode
        if (token == NULL)
        {
            return &(get_log_entry(currDentry->inode_number)->inode);
        }
        else
        {
            logEntry = get_log_entry(currDentry->inode_number);
        }
    }

    return NULL;
}

// Fuse below:
static int wfs_getattr(const char *path, struct stat *stbuf)
{
    struct wfs_inode *inode = get_inode_by_path(path);
    if (inode == NULL)
    {
        return -ENOENT;
    }

    stbuf->st_mode = inode->mode;
    stbuf->st_nlink = inode->links;
    stbuf->st_size = inode->size;
    stbuf->st_uid = inode->uid;
    stbuf->st_gid = inode->gid;
    stbuf->st_atime = inode->atime;
    stbuf->st_mtime = inode->mtime;
    stbuf->st_ctime = inode->ctime;

    return 0;
}

static int wfs_mknod(const char *path, mode_t mode, dev_t rdev)
{
    struct wfs_inode *inode = get_inode_by_path(path);
    if (inode != NULL)
    {
        return -EEXIST;
    }

    struct wfs_sb *sb = (struct wfs_sb *)mapped_disk;
    struct wfs_log_entry *new_log_entry = (struct wfs_log_entry *)((char *)mapped_disk + sb->head);
    struct wfs_inode *new_inode = &new_log_entry->inode;
    int nextInode = -1;
    int nextInodeNum = 1;

    // Loop through all possible inode numbers
    while (nextInodeNum <= UINT32_MAX)
    {
        bool inodeAvailable = true;
        char *logEntryAddr = (char *)mapped_disk + sizeof(struct wfs_sb);

        // Traverse each log entry to check if the inode number is in use
        while (logEntryAddr - (char *)mapped_disk < sb->head)
        {
            struct wfs_log_entry *logEntry = (struct wfs_log_entry *)logEntryAddr;
            if (logEntry->inode.inode_number == nextInodeNum && logEntry->inode.deleted == 0)
            {
                inodeAvailable = false;
                break;
            }

            logEntryAddr += sizeof(struct wfs_inode) + logEntry->inode.size;
        }

        if (inodeAvailable)
        {
            nextInode = nextInodeNum; // Assign the result to nextInode
            // Use nextInode as needed
            break; // Exit the loop since you've found the next free inode
        }
        nextInodeNum++;
    }
    new_inode->inode_number = nextInode;
    new_inode->deleted = 0;
    new_inode->mode = mode;
    struct fuse_context *fuse = fuse_get_context();
    new_inode->uid = fuse->uid;
    new_inode->gid = fuse->gid;
    new_inode->flags = 0;
    new_inode->size = 0;
    new_inode->atime = time(NULL);
    new_inode->mtime = time(NULL);
    new_inode->ctime = time(NULL);
    new_inode->links = 1;

    // Update superblock and disk image size
    sb->head += sizeof(struct wfs_inode);
    mapped_size += sizeof(struct wfs_inode);

    char *copied_path = strdup(path);           // Duplicate the original path
    char parent_path[MAX_PATH_LEN] = "/";       // Initialize the parent directory path as root
    char *token = strtok(copied_path, "/");     // Tokenize the copied path with '/' as the delimiter
    char last_token[MAX_FILE_NAME_LEN] = ""; // Initialize the last segment of the path as an empty string

    if (token)
    {
        strncpy(last_token, token, MAX_FILE_NAME_LEN - 1); // Copy the first token to last_token
    }

    while (token)
    {
        token = strtok(NULL, "/"); // Continue tokenizing the path
        if (token != NULL)
        {
            strncat(parent_path, last_token, MAX_PATH_LEN - strlen(parent_path) - 1); // Append the previous token to parent_path
            strncat(parent_path, "/", MAX_PATH_LEN - strlen(parent_path) - 1);           // Append a '/' to parent_path
            strncpy(last_token, token, MAX_FILE_NAME_LEN - 1);                        // Copy the current token to last_token
        }
    }

    struct wfs_log_entry *old_dir_entry = (strcmp(parent_path, "/") == 0) ? get_log_entry(0) : get_log_entry(get_inode_by_path(parent_path)->inode_number);
    struct wfs_inode *old_dir_inode = &old_dir_entry->inode;

    struct wfs_log_entry *new_dir_entry = (struct wfs_log_entry *)((char *)mapped_disk + sb->head);
    struct wfs_inode *new_dir_inode = &new_dir_entry->inode;

    copy_data(new_dir_inode, old_dir_inode, 1);

    int numDE = old_dir_inode->size / sizeof(struct wfs_dentry);
    struct wfs_dentry *currDE = (struct wfs_dentry *)old_dir_entry->data;
    struct wfs_dentry *newCurrDE = (struct wfs_dentry *)new_dir_entry->data;
    for (int i = 0; i < numDE; i++)
    {
        memcpy(newCurrDE, currDE, sizeof(struct wfs_dentry));
        newCurrDE++;
        currDE++;
    }

    struct wfs_dentry *newDE = (struct wfs_dentry *)new_dir_entry->data + numDE;
    memcpy(newDE->name, last_token, MAX_FILE_NAME_LEN);
    newDE->inode_number = nextInode;

    sb->head += sizeof(struct wfs_log_entry) + new_dir_inode->size;
    mapped_size += sizeof(struct wfs_log_entry) + new_dir_inode->size;

    free(copied_path);

    return 0;
}

static int wfs_mkdir(const char *path, mode_t mode)
{
    return wfs_mknod(path, S_IFDIR, 0);
}

static int wfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    struct wfs_inode *f_inode = get_inode_by_path(path);
    if (f_inode == NULL)
    {
        return -ENOENT;
    }
    struct wfs_log_entry *f_entry = get_log_entry(f_inode->inode_number);
    f_inode->deleted = 1;

    // New log
    struct wfs_sb *sb = (struct wfs_sb *)mapped_disk;
    struct wfs_log_entry *new_entry = (struct wfs_log_entry *)((char *)mapped_disk + sb->head);
    struct wfs_inode *new_inode = &new_entry->inode;
    copy_data(new_inode, f_inode, 2);
    memcpy((void *)new_entry->data, (void *)f_entry->data, f_inode->size);

    // write data to file
    char *new_data = new_entry->data;
    new_data += offset;
    memcpy(new_data, buf, size);

    unsigned int new_size = (new_data + size) - new_entry->data;
    if (new_size > f_inode->size)
    {
        new_inode->size = new_size;
    }
    else
    {
        new_inode->size = f_inode->size;
    }

    sb->head += sizeof(struct wfs_inode) + new_inode->size;
    mapped_size += sizeof(struct wfs_inode) + new_inode->size;

    return size;
}

static int wfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{

    struct wfs_log_entry *curr_entry = get_log_entry(get_inode_by_path(path)->inode_number);
    size = curr_entry->inode.size;
    memcpy(buf + offset, curr_entry->data, sizeof(char) * size);

    return size;
}

static int wfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{

    struct wfs_inode *dir_inode;
    struct wfs_log_entry *dir_entry;

    dir_inode = get_inode_by_path(path);
    dir_entry = get_log_entry(dir_inode->inode_number);

    if (dir_inode == NULL)
    {
        return -ENOENT;
    }

    int numDE = dir_inode->size / sizeof(struct wfs_dentry);
    struct wfs_dentry *curr_DE = (struct wfs_dentry *)dir_entry->data;
    for (int i = 0; i < numDE; i++)
    {
        filler(buf, curr_DE->name, NULL, 0);

        curr_DE++;
    }
    return 0;
}

static int wfs_unlink(const char *path)
{

    struct wfs_sb *sb = (struct wfs_sb *)mapped_disk;
    struct wfs_inode *inode_del = get_inode_by_path(path);


    inode_del->deleted = 1;

    char *copied_path = strdup(path);
    char parent_path[MAX_PATH_LEN] = "/";
    char *token = strtok(copied_path, "/");
    char last_token[MAX_FILE_NAME_LEN] = "";
    if (token)
    {
        strncpy(last_token, token, MAX_FILE_NAME_LEN - 1);
    }
    while (token)
    {
        token = strtok(NULL, "/");
        if (token != NULL)
        {
            strncat(parent_path, last_token, MAX_PATH_LEN - strlen(parent_path) - 1);
            strncat(parent_path, "/", MAX_PATH_LEN - strlen(parent_path) - 1);
            strncpy(last_token, token, MAX_FILE_NAME_LEN - 1);
        }
    }

    struct wfs_log_entry *old_dir_entry = (strcmp(parent_path, "/") == 0) ? get_log_entry(0) : get_log_entry(get_inode_by_path(parent_path)->inode_number);

    struct wfs_inode *old_dir_inode = &old_dir_entry->inode;
    struct wfs_log_entry *new_dir_entry = (struct wfs_log_entry *)((char *)mapped_disk + sb->head);
    struct wfs_inode *new_dir_inode = &new_dir_entry->inode;
    copy_data(new_dir_inode, old_dir_inode, 0);

    int numDE = old_dir_inode->size / sizeof(struct wfs_dentry);
    struct wfs_dentry *currDE = (struct wfs_dentry *)old_dir_entry->data;
    struct wfs_dentry *newCurrDE = (struct wfs_dentry *)new_dir_entry->data;
    for (int i = 0; i < numDE; i++)
    {
        if (currDE->inode_number != inode_del->inode_number)
        {
            memcpy(newCurrDE, currDE, sizeof(struct wfs_dentry));
            newCurrDE++;
        }
        currDE++;
    }

    sb->head += sizeof(struct wfs_log_entry) + new_dir_inode->size;
    mapped_size += sizeof(struct wfs_log_entry) + new_dir_inode->size;

    free(copied_path);

    return 0;
}

static struct fuse_operations wfs_oper = {
    .getattr = wfs_getattr,
    .mknod = wfs_mknod,
    .mkdir = wfs_mkdir,
    .write = wfs_write,
    .read = wfs_read,
    .readdir = wfs_readdir,
    .unlink = wfs_unlink,
};

int main(int argc, char *argv[])
{
    if (argc < 4)
    {
        fprintf(stderr, "usage: %s <disk_path> <mount_point>\n", argv[0]);
        return 1;
    }

    disk_path = argv[argc - 2];
    char *mount_point = argv[argc - 1];

    disk = fopen(disk_path, "rb+");
    if (!disk)
    {
        perror("Unable to open disk image");
        return 1;
    }

    fseek(disk, 0, SEEK_END);
    mapped_size = ftell(disk);
    fseek(disk, 0, SEEK_SET);

    mapped_disk = mmap(NULL, mapped_size, PROT_READ | PROT_WRITE, MAP_SHARED, fileno(disk), 0);
    if (mapped_disk == MAP_FAILED)
    {
        perror("Error mapping disk image");
        fclose(disk);
        return 1;
    }

    int fuse_argc = argc - 1;
    char *fuse_argv[fuse_argc];
    for (int i = 0; i < fuse_argc - 1; ++i)
    {
        fuse_argv[i] = argv[i];
    }
    fuse_argv[fuse_argc - 1] = mount_point;

    fuse_main(fuse_argc, fuse_argv, &wfs_oper, NULL);
    munmap(mapped_disk, mapped_size);
    fclose(disk);

    return 0;
}