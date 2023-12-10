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

FILE *disk_image = NULL;
const char *disk_path;
void *mapped_disk_image = NULL;
size_t mapped_disk_image_size;



void copy_data(struct wfs_inode *newInode, struct wfs_inode *fileInode, int condition)
{
    newInode->inode_number = fileInode->inode_number;
    newInode->deleted = 0;
    newInode->mode = fileInode->mode;
    newInode->uid = fileInode->uid;
    newInode->gid = fileInode->gid;
    newInode->flags = fileInode->flags;
    newInode->atime = time(NULL);
    newInode->ctime = time(NULL);
    newInode->mtime = time(NULL);
    if (condition == 1)
    {
        newInode->size = fileInode->size + sizeof(struct wfs_dentry);
    }
    else if (condition == 0)
    {
        newInode->size = fileInode->size - sizeof(struct wfs_dentry);
    }
    else
    {
    }
}


// Retrieves the root log entry from the memory-mapped disk image.
struct wfs_log_entry *get_entry(int inode_number)
{
    char *currAddr = (char *)mapped_disk_image + sizeof(struct wfs_sb);
    struct wfs_sb *sb = (struct wfs_sb *)mapped_disk_image;
    struct wfs_log_entry *actualInode = NULL;
    while (currAddr < ((char *)mapped_disk_image + sb->head))
    {
        struct wfs_log_entry *logEntry = (struct wfs_log_entry *)currAddr;

        if (logEntry->inode.inode_number == inode_number)
        {
            actualInode = logEntry;
        }

        // Check if the current entry is the root entry
        currAddr += sizeof(struct wfs_inode) + logEntry->inode.size;
    }

    return actualInode;
}

struct wfs_dentry *find_dentry(struct wfs_log_entry *entry, const char *name)
{

    int numDentries = entry->inode.size / sizeof(struct wfs_dentry);
    if (numDentries == 0)
    {
        return NULL;
    }
    struct wfs_dentry *currDentry = (struct wfs_dentry *)entry->data;

    // Iterate over each directory entry
    for (int i = 0; i < numDentries; i++)
    {
        if (strcmp(currDentry->name, name) == 0)
        {
            return currDentry;
        }
        currDentry++;
    }
    return NULL;
}

struct wfs_inode *find_inode_by_path(const char *path)
{

    struct wfs_log_entry *logEntry = get_entry(0);
    char copyOfPath[strlen(path)];
    strcpy(copyOfPath, path);
    char *token = strtok(copyOfPath, "/");

    while (token)
    {
        int numDentries = logEntry->inode.size / sizeof(struct wfs_dentry);
        struct wfs_dentry *currDentry = find_dentry(logEntry, token);
        if (currDentry == NULL || numDentries == 0)
        {
            return NULL;
        }
        token = strtok(NULL, "/");

        // If it was the last token in the path, return the corresponding inode
        if (token == NULL)
        {
            return &(get_entry(currDentry->inode_number)->inode);
        }
        else
        {
            logEntry = get_entry(currDentry->inode_number);
        }
    }

    return NULL;
}

// Fuse below:
static int wfs_getattr(const char *path, struct stat *stbuf)
{
    memset(stbuf, 0, sizeof(struct stat));

    if (strcmp(path, "/") == 0)
    {
        stbuf->st_mode = S_IFDIR;
        stbuf->st_nlink = 2;
    }
    else
    {
        struct wfs_inode *inode = find_inode_by_path(path);
        if (!inode)
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
    }

    return 0;
}

static int wfs_mknod(const char *path, mode_t mode, dev_t rdev)
{
    struct wfs_inode *inode = find_inode_by_path(path);
    if (inode != NULL)
    {
        return -EEXIST;
    }
    else
    {
        struct wfs_sb *sb = (struct wfs_sb *)mapped_disk_image;
        struct wfs_log_entry *newLogEntry = (struct wfs_log_entry *)((char *)mapped_disk_image + sb->head);
        struct wfs_inode *newInode = &newLogEntry->inode;
        int nextInode = -1;
        struct wfs_sb *superBlock = (struct wfs_sb *)mapped_disk_image;
        int nextInodeNum = 1;

        // Loop through all possible inode numbers
        while (nextInodeNum <= UINT32_MAX)
        {
            bool inodeAvailable = true;
            char *logEntryAddr = (char *)mapped_disk_image + sizeof(struct wfs_sb);

            // Traverse each log entry to check if the inode number is in use
            while (logEntryAddr - (char *)mapped_disk_image < superBlock->head)
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
        newInode->inode_number = nextInode;
        newInode->deleted = 0;
        newInode->mode = mode;
        struct fuse_context *fuse = fuse_get_context();
        newInode->uid = fuse->uid;
        newInode->gid = fuse->gid;
        newInode->flags = 0;
        newInode->size = 0;
        newInode->atime = time(NULL);
        newInode->mtime = time(NULL);
        newInode->ctime = time(NULL);
        newInode->links = 1;

        // Update superblock and disk image size
        sb->head += sizeof(struct wfs_inode);
        mapped_disk_image_size += sizeof(struct wfs_inode);

        char *copyOfPath = strdup(path);
        char dirPath[MAX_PATH_LEN] = "/";
        char *token = strtok(copyOfPath, "/");
        char last[MAX_FILE_NAME_LEN] = "";
        if (token)
        {
            strncpy(last, token, MAX_FILE_NAME_LEN - 1);
        }
        while (token)
        {
            token = strtok(NULL, "/");
            if (token != NULL)
            {
                strncat(dirPath, last, MAX_PATH_LEN - strlen(dirPath) - 1);
                strncat(dirPath, "/", MAX_PATH_LEN - strlen(dirPath) - 1);
                strncpy(last, token, MAX_FILE_NAME_LEN - 1);
            }
        }

        struct wfs_log_entry *oldDirEntry = (strcmp(dirPath, "/") == 0) ? get_entry(0) : get_entry(find_inode_by_path(dirPath)->inode_number);

        struct wfs_inode *oldDirInode = &oldDirEntry->inode;
        struct wfs_log_entry *newDirEntry = (struct wfs_log_entry *)((char *)mapped_disk_image + sb->head);
        struct wfs_inode *newDirInode = &newDirEntry->inode;
        copy_data(newDirInode, oldDirInode, 1);

        // Copy directory entries excluding the deleted one
        int numDentries = oldDirInode->size / sizeof(struct wfs_dentry);
        struct wfs_dentry *currDentry = (struct wfs_dentry *)oldDirEntry->data;
        struct wfs_dentry *newCurrDentry = (struct wfs_dentry *)newDirEntry->data;
        for (int i = 0; i < numDentries; i++)
        {
            memcpy(newCurrDentry, currDentry, sizeof(struct wfs_dentry));
            newCurrDentry++;
            currDentry++;
        }

        struct wfs_dentry *newDentry = (struct wfs_dentry *)newDirEntry->data + numDentries;
        memcpy(newDentry->name, last, MAX_FILE_NAME_LEN);
        newDentry->inode_number = nextInode;
        // Update superblock and disk image size
        sb->head += sizeof(struct wfs_log_entry) + newDirInode->size;
        mapped_disk_image_size += sizeof(struct wfs_log_entry) + newDirInode->size;

        free(copyOfPath);
    }

    return 0; // or appropriate error code
}

static int wfs_mkdir(const char *path, mode_t mode)
{
    return wfs_mknod(path, S_IFDIR, 0);
}

static int wfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    struct wfs_inode *fileInode = find_inode_by_path(path);
    if (fileInode == NULL)
    {
        return -ENOENT;
    }
    struct wfs_log_entry *fileEntry = get_entry(fileInode->inode_number);
    // invalidate old one
    fileInode->deleted = 1;
    // Append a new log entry with the updated file content
    struct wfs_sb *sb = (struct wfs_sb *)mapped_disk_image;
    struct wfs_log_entry *newEntry = (struct wfs_log_entry *)((char *)mapped_disk_image + sb->head);
    struct wfs_inode *newInode = &newEntry->inode;
    copy_data(newInode, fileInode, 2);
    // copy over data
    memcpy((void *)newEntry->data, (void *)fileEntry->data, fileInode->size);
    // write new data to file
    char *newData = newEntry->data;
    newData += offset;
    memcpy(newData, buf, size);

    // compute new size
    unsigned int newWriteSize = (newData + size) - newEntry->data;
    if (newWriteSize > fileInode->size)
    {
        newInode->size = newWriteSize;
    }
    else
    {
        newInode->size = fileInode->size;
    }

    sb->head += sizeof(struct wfs_inode) + newInode->size;
    mapped_disk_image_size += sizeof(struct wfs_inode) + newInode->size;

    return size; // return the number of bytes written or an error code
}

static int wfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{

    struct wfs_log_entry *currEntry = get_entry(find_inode_by_path(path)->inode_number);
    size = currEntry->inode.size;
    memcpy(buf + offset, currEntry->data, sizeof(char) * size);

    return size;
}

static int wfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{

    struct wfs_inode *dirInode;
    struct wfs_log_entry *dirEntry;
    if (strcmp(path, "/") == 0)
    {
        dirEntry = get_entry(0);
        dirInode = &dirEntry->inode;
    }
    else
    {
        dirInode = find_inode_by_path(path);
        dirEntry = get_entry(dirInode->inode_number);
    }

    if (dirInode == NULL)
    {
        return -ENOENT;
    }

    // For each entry in the directory, use filler() to add it to the list
    int numDentries = dirInode->size / sizeof(struct wfs_dentry);
    struct wfs_dentry *currDentry = (struct wfs_dentry *)dirEntry->data;
    for (int i = 0; i < numDentries; i++)
    {
        filler(buf, currDentry->name, NULL, 0);

        currDentry++;
    }
    return 0; // or appropriate error code
}

static int wfs_unlink(const char *path)
{

    struct wfs_sb *sb = (struct wfs_sb *)mapped_disk_image;
    struct wfs_inode *inodeToDelete = find_inode_by_path(path);

    // Mark the file's inode as deleted
    inodeToDelete->deleted = 1;
    // Process directory path
    char *copyOfPath = strdup(path);
    char dirPath[MAX_PATH_LEN] = "/";
    char *token = strtok(copyOfPath, "/");
    char last[MAX_FILE_NAME_LEN] = "";
    if (token)
    {
        strncpy(last, token, MAX_FILE_NAME_LEN - 1);
    }
    while (token)
    {
        token = strtok(NULL, "/");
        if (token != NULL)
        {
            strncat(dirPath, last, MAX_PATH_LEN - strlen(dirPath) - 1);
            strncat(dirPath, "/", MAX_PATH_LEN - strlen(dirPath) - 1);
            strncpy(last, token, MAX_FILE_NAME_LEN - 1);
        }
    }

    struct wfs_log_entry *oldDirEntry = (strcmp(dirPath, "/") == 0) ? get_entry(0) : get_entry(find_inode_by_path(dirPath)->inode_number);

    struct wfs_inode *oldDirInode = &oldDirEntry->inode;
    struct wfs_log_entry *newDirEntry = (struct wfs_log_entry *)((char *)mapped_disk_image + sb->head);
    struct wfs_inode *newDirInode = &newDirEntry->inode;
    copy_data(newDirInode, oldDirInode, 0);

    // Copy directory entries excluding the deleted one
    int numDentries = oldDirInode->size / sizeof(struct wfs_dentry);
    struct wfs_dentry *currDentry = (struct wfs_dentry *)oldDirEntry->data;
    struct wfs_dentry *newCurrDentry = (struct wfs_dentry *)newDirEntry->data;
    for (int i = 0; i < numDentries; i++)
    {
        if (currDentry->inode_number != inodeToDelete->inode_number)
        {
            memcpy(newCurrDentry, currDentry, sizeof(struct wfs_dentry));
            newCurrDentry++;
        }
        currDentry++;
    }

    // Update superblock and disk image size
    sb->head += sizeof(struct wfs_log_entry) + newDirInode->size;
    mapped_disk_image_size += sizeof(struct wfs_log_entry) + newDirInode->size;

    free(copyOfPath);

    return 0; // Success
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

    disk_image = fopen(disk_path, "rb+");
    if (!disk_image)
    {
        perror("Unable to open disk image");
        return 1;
    }

    fseek(disk_image, 0, SEEK_END);
    mapped_disk_image_size = ftell(disk_image);
    fseek(disk_image, 0, SEEK_SET);

    mapped_disk_image = mmap(NULL, mapped_disk_image_size, PROT_READ | PROT_WRITE, MAP_SHARED, fileno(disk_image), 0);
    if (mapped_disk_image == MAP_FAILED)
    {
        perror("Error mapping disk image");
        fclose(disk_image);
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
    munmap(mapped_disk_image, mapped_disk_image_size);
    fclose(disk_image);

    return 0;
}