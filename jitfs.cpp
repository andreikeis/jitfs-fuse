
#define FUSE_USE_VERSION 30

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <cstddef>

#include "easylogging++.h"
#include "sqlite3.h"

INITIALIZE_EASYLOGGINGPP

struct global_config {
    blksize_t blksize;
    std::string cache;
    std::string db_filename;
    std::string jitfs_srv_sock;

    // Communication with the server is UDP over unix domain sockets.
    struct sockaddr_un jitfs_srv_addr;
    // Communication socket, bind to reply address.
    int sock;

    sqlite3 *db;
    sqlite3_stmt *select_stmt;
};

static global_config global_cfg;

static int _lstat(const char *path, struct stat *out) 
{
    // path - /<sha256>
    if (!path[1]) {
        if (-1 == lstat(global_cfg.cache.c_str(), out)) {
		    return -errno;
        }
        return 0;
    }

    int rc;
    rc = sqlite3_bind_text(global_cfg.select_stmt, 1, path + 1, -1, NULL);
    rc = sqlite3_step(global_cfg.select_stmt);

    if (rc == SQLITE_DONE) {
        LOG(DEBUG) << "row not found, ENOENT";
        return -ENOENT;
    }

    int mode = sqlite3_column_int(global_cfg.select_stmt, 0);
    int size = sqlite3_column_int(global_cfg.select_stmt, 1);

    LOG(DEBUG) << "Metadata: " << path << 
        ", size: " << size << ", mode: " << mode;
    sqlite3_clear_bindings(global_cfg.select_stmt);
    sqlite3_reset(global_cfg.select_stmt);

    struct stat fstat = {0};
    
    fstat.st_ino = (ino_t) rand();
    fstat.st_nlink = 1;
    fstat.st_uid = 1;
    fstat.st_gid = 1;
    fstat.st_blksize = global_cfg.blksize;

    fstat.st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
    if (mode) {
        fstat.st_mode |= S_IXUSR | S_IXGRP | S_IXOTH;
    }

    std::string path_s(path);
    fstat.st_size = (off_t) size;

    LOG(DEBUG) << "stat: " 
        << " .st_mode: " << fstat.st_mode
        << " .st_size: " << fstat.st_size;

    memcpy(out, &fstat, sizeof(fstat));
    return 0;
}

static std::string cache_path(const char *path) 
{
    // path is /<sha256>
    std::string checksum(path + 1);
    std::stringstream str;
    str << global_cfg.cache << "/"
        << checksum.substr(0, 2) << "/"
        << checksum.substr(2, 2) << "/" << checksum;
    
    LOG(DEBUG) << "cache path: " << str.str();
    std::string out_path = str.str();

    if (access(out_path.c_str(), F_OK) != 0) {
        struct sockaddr *serv_addr = 
            (struct sockaddr *) &global_cfg.jitfs_srv_addr;
        socklen_t serv_addr_len = sizeof(global_cfg.jitfs_srv_addr);

        LOG(DEBUG) << "requesting checksum: " << checksum;
        int rc = sendto(
                global_cfg.sock, checksum.c_str(), checksum.size(),
                0, serv_addr, serv_addr_len);
        if (rc == -1) {
            LOG(ERROR) << "Unable to request checksum: "
                << strerror(errno);
        }
        else {
            char reply[16];
            rc = recvfrom(
                global_cfg.sock, reply, sizeof(reply),
                0, serv_addr, &serv_addr_len);
            LOG(DEBUG) << "Got back: " << int(reply[0]);
        }
    }
    return out_path;
}

static void *jitfs_init(struct fuse_conn_info *conn,
        struct fuse_config *cfg)
{
    LOG(DEBUG) << "init.";
	(void) conn;
	cfg->use_ino = 0;
    cfg->readdir_ino = 0;

	/* Pick up changes from lower filesystem right away. This is
	   also necessary for better hardlink support. When the kernel
	   calls the unlink() handler, it does not know the inode of
	   the to-be-removed entry and can therefore not invalidate
	   the cache of the associated inode - resulting in an
	   incorrect st_nlink value being reported for any remaining
	   hardlinks to this inode. */
	cfg->entry_timeout = 0;
	cfg->attr_timeout = 0;
	cfg->negative_timeout = 0;

	return NULL;
}

static int jitfs_getattr(const char *path, struct stat *stbuf,
        struct fuse_file_info *fi)
{
    LOG(DEBUG) << "getattr: " << path;

	(void) fi;
    return _lstat(path, stbuf);
}

static int jitfs_access(const char *path, int mask)
{
    LOG(DEBUG) << "access: " << path;
    return 0;
}

static int jitfs_readlink(const char *path, char *buf, size_t size)
{
    LOG(DEBUG) << "readlink: " << path;
    return -EINVAL;
}


static int jitfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
        off_t offset, struct fuse_file_info *fi,
        enum fuse_readdir_flags flags)
{
    LOG(DEBUG) << "readdir: " << path;
    return 0;
}

static int jitfs_mknod(const char *path, mode_t mode, dev_t rdev)
{
    LOG(DEBUG) << "mknod: " << path;
    return -EACCES;	
}

static int jitfs_mkdir(const char *path, mode_t mode)
{
    LOG(DEBUG) << "mkdir: " << path;
    return -EACCES;	
}

static int jitfs_unlink(const char *path)
{
    LOG(DEBUG) << "unlink: " << path;
    return -EACCES;	
}

static int jitfs_rmdir(const char *path)
{
    LOG(DEBUG) << "rmdir: " << path;
    return -EACCES;	
}

static int jitfs_symlink(const char *from, const char *to)
{
    LOG(DEBUG) << "symlink: from: " << from << " to " << to;
    return -EACCES;
}

static int jitfs_rename(const char *from, const char *to, unsigned int flags)
{
    LOG(DEBUG) << "rename: from: " << from << " to " << to;
    return -EACCES;
}

static int jitfs_link(const char *from, const char *to)
{
    LOG(DEBUG) << "link: from: " << from << " to " << to;
    return -EACCES;
}

static int jitfs_chmod(const char *path, mode_t mode,
		     struct fuse_file_info *fi)
{
    LOG(DEBUG) << "chmod: " << path;
    return -EACCES;	
}

static int jitfs_chown(const char *path, uid_t uid, gid_t gid,
		     struct fuse_file_info *fi)
{
    LOG(DEBUG) << "chown: " << path;
    return -EACCES;	
}

static int jitfs_truncate(const char *path, off_t size,
			struct fuse_file_info *fi)
{
    LOG(DEBUG) << "truncate: " << path;
    return -EACCES;	
}

static int jitfs_create(const char *path, mode_t mode,
		      struct fuse_file_info *fi)
{
    LOG(DEBUG) << "create: " << path;
    return -EACCES;	
}

static int jitfs_open(const char *path, struct fuse_file_info *fi)
{
    LOG(DEBUG) << "open: " << path;

	int res = open(cache_path(path).c_str(), fi->flags);
	if (res == -1)
		return -errno;

	fi->fh = res;
	return 0;
}

static int jitfs_read(const char *path, char *buf, size_t size, off_t offset,
		    struct fuse_file_info *fi)
{
    LOG(DEBUG) << "read: " << path;

	int fd;

	if(fi == NULL)
		fd = open(cache_path(path).c_str(), O_RDONLY);
	else
		fd = fi->fh;
	
	if (fd == -1)
		return -errno;

	int res = pread(fd, buf, size, offset);
	if (res == -1)
		res = -errno;

	if (fi == NULL)
		close(fd);
	return res;
}

static int jitfs_write(const char *path, const char *buf, size_t size,
		     off_t offset, struct fuse_file_info *fi)
{
    LOG(DEBUG) << "write: " << path;
    return -EACCES;	
}

static int jitfs_statfs(const char *path, struct statvfs *stbuf)
{
    LOG(DEBUG) << "statfs: " << path;

	int res = statvfs(path, stbuf);
	if (res == -1)
		return -errno;

	return 0;
}

static int jitfs_release(const char *path, struct fuse_file_info *fi)
{
    LOG(DEBUG) << "release: " << path;
	(void) path;
	close(fi->fh);
	return 0;
}

static int jitfs_fsync(const char *path, int isdatasync,
		     struct fuse_file_info *fi)
{
	/* Just a stub.	 This method is optional and can safely be left
	   unimplemented */
    LOG(DEBUG) << "fsync: " << path;
	(void) path;
	(void) isdatasync;
	(void) fi;
	return 0;
}

static void init_fuse_operations(struct fuse_operations &op) 
{
    op.init         = jitfs_init;
    op.getattr	    = jitfs_getattr;
    op.access		= jitfs_access;
    op.readlink	    = jitfs_readlink;
    op.readdir      = jitfs_readdir;
    op.mknod		= jitfs_mknod;
    op.mkdir		= jitfs_mkdir;
    op.symlink	    = jitfs_symlink;
    op.unlink		= jitfs_unlink;
    op.rmdir		= jitfs_rmdir;
    op.rename		= jitfs_rename;
    op.link		    = jitfs_link;
    op.chmod		= jitfs_chmod;
    op.chown		= jitfs_chown;
    op.truncate	    = jitfs_truncate;
    op.open		    = jitfs_open;
    op.create 	    = jitfs_create;
    op.read		    = jitfs_read;
    op.write		= jitfs_write;
    op.statfs		= jitfs_statfs;
    op.release	    = jitfs_release;
    op.fsync		= jitfs_fsync;
    return;
}

struct jitfs_config {
     char *cache;
     char *db;
     char *sock;
};

enum {
     KEY_HELP,
     KEY_VERSION,
};

#define JITFS_OPT(t, p, v) { t, offsetof(struct jitfs_config, p), v }

static struct fuse_opt jitfs_opts[] = {
     JITFS_OPT("cache=%s",          cache, 0),
     JITFS_OPT("db=%s",             db, 0),
     JITFS_OPT("sock=%s",           sock, 0),

     FUSE_OPT_KEY("-V",             KEY_VERSION),
     FUSE_OPT_KEY("--version",      KEY_VERSION),
     FUSE_OPT_KEY("-h",             KEY_HELP),
     FUSE_OPT_KEY("--help",         KEY_HELP),
     FUSE_OPT_END
};

static struct fuse_operations jitfs_operations = {0};

static int jitfs_opt_proc(void *data, const char *arg, int key, 
        struct fuse_args *outargs)
{
    switch (key) {
        case KEY_HELP:
            std::cerr << 
                "usage: " << outargs->argv[0] << "  mountpoint [options]\n"
                "\n"
                "general options:\n"
                "    -o opt,[opt...]  mount options\n"
                "    -h   --help      print help\n"
                "    -V   --version   print version\n"
                "\n"
                "jitfs options:\n"
                "    -o cache=STRING  path to jitfs cache\n"
                "    -o sock=STRING   path to jitfs server socket\n"
                "    -o db=STRING     path to jitfs db\n";

            fuse_opt_add_arg(outargs, "-ho");
            fuse_main(outargs->argc, outargs->argv, &jitfs_operations, NULL);
            exit(1);

        case KEY_VERSION:
            std::cerr << "jitfs version: <unknown>" << std::endl;
            fuse_opt_add_arg(outargs, "--version");
            fuse_main(outargs->argc, outargs->argv, &jitfs_operations, NULL);
            exit(0);
    }
    return 1;
}

void init_log() 
{
    el::Configurations log_conf;
    log_conf.setToDefault();
    log_conf.set(
            el::Level::Info, 
            el::ConfigurationType::Format, "%datetime %level %msg");
    log_conf.set(
            el::Level::Debug, 
            el::ConfigurationType::Format, "%datetime %level [%loc] %msg");
    el::Loggers::reconfigureAllLoggers(log_conf);
    log_conf.clear();
}

int _init_db()
{
    int rc = sqlite3_open_v2(
            global_cfg.db_filename.c_str(), 
            &global_cfg.db, 
            SQLITE_OPEN_READONLY, 
            NULL);
    if (rc != 0) {
        LOG(ERROR) << "Unable to open db: " << global_cfg.db_filename
            << ", rc: " << rc;
        return -1;
    }
    rc = sqlite3_prepare_v2(
            global_cfg.db,
            "SELECT mode, size FROM files WHERE checksum = ?",
            -1,
            &global_cfg.select_stmt,
            NULL);
    if (rc != 0) {
        LOG(ERROR) << "Unable to prepare db statement, rc = " << rc;
        return -1;
    }
    
    return 0;
}

int _init_stat()
{
    struct stat stbuf = {0};
    int rc = lstat(global_cfg.cache.c_str(), &stbuf);
    if (rc != 0) {
        LOG(ERROR) << "Error: lstat " << global_cfg.cache 
            << ", errno: " << errno;
        return -1;
    }
    global_cfg.blksize = stbuf.st_blksize;
    return 0;
}

int _make_un_addr(const char *path, struct sockaddr_un *addr)
{
    bzero(addr, sizeof(*addr));
    addr->sun_family = AF_UNIX;
    strncpy(addr->sun_path, path, sizeof(addr->sun_path) - 1);
}

int _init_sock()
{
    struct sockaddr_un server_addr;
    _make_un_addr(global_cfg.jitfs_srv_sock.c_str(), &server_addr);
    global_cfg.jitfs_srv_addr = server_addr;
   
    // Ensure socket path does not exist.
    std::stringstream reply_sock_str;
    reply_sock_str << global_cfg.jitfs_srv_sock << ".reply." << getpid();
    const std::string reply_sock = reply_sock_str.str();
    
    struct sockaddr_un reply_addr;
    _make_un_addr(reply_sock.c_str(), &reply_addr);
    
    // create socket
    int sock = socket(PF_UNIX, SOCK_DGRAM, 0);
    if (!sock) {
        LOG(ERROR) << "Error creating socket, errno: " << errno;
        return -1;
    }

    // Bind socket to reply address.
    unlink(reply_sock.c_str());
    int rc = bind(sock, (const struct sockaddr *)&reply_addr, sizeof(reply_addr));
    if (rc != 0) {
        LOG(ERROR) << "bind " << reply_sock
            << ": " << strerror(errno);
        return -1;
    }

    global_cfg.sock = sock;
    return 0;
}

int main(int argc, char *argv[])
{
    init_log();

    LOG(INFO) << "Starting jitfs.";
	umask(0);
    
    init_fuse_operations(jitfs_operations);

    fuse_args args = FUSE_ARGS_INIT(argc, argv);
    struct jitfs_config conf = {0};

    fuse_opt_parse(&args, &conf, jitfs_opts, jitfs_opt_proc);

    if (conf.cache && conf.cache[0]) {
        global_cfg.cache = conf.cache;
    }
    if (conf.db && conf.db[0]) {
        global_cfg.db_filename = conf.db;
    }
    if (conf.sock && conf.sock[0]) {
        global_cfg.jitfs_srv_sock = conf.sock;
    }

    if (_init_stat() != 0) {
        LOG(ERROR) << "Error initializing filesystem.";
        return -1;
    }

    if (_init_db() != 0) {
        LOG(ERROR) << "Error initializing db.";
        return -1;
    }
 
    if (global_cfg.jitfs_srv_sock[0]) {
        if (_init_sock() != 0) {
            LOG(ERROR) << "Error initializing socket.";
            return -1;
        }
    }
    
    return fuse_main(args.argc, args.argv, &jitfs_operations, NULL);
}
