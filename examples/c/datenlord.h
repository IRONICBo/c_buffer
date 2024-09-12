#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

/// The node ID of the root inode
constexpr static const uint64_t ROOT_ID = 1;

/// Whether to check permission.
/// If fuse mount with `-o default_permissions`, then we should not check
/// permission. Otherwise, we should check permission.
/// TODO: add a feature flag to control this
constexpr static const bool NEED_CHECK_PERM = false;

struct datenlord_bytes {
  const uint8_t *data;
  uintptr_t len;
};

struct datenlord_error {
  unsigned int code;
  datenlord_bytes message;
};

extern "C" {

datenlord_error *init(const char *config);

bool exists(const char *dir_path);

datenlord_error *mkdir(const char *dir_path);

datenlord_error *delete_dir(const char *dir_path, bool recursive);

datenlord_error *rename_path(const char *src_path, const char *dest_path);

datenlord_error *copy_from_local_file(bool overwrite,
                                      const char *local_file_path,
                                      const char *dest_file_path);

datenlord_error *copy_to_local_file(const char *src_file_path, const char *local_file_path);

datenlord_error *stat(const char *file_path);

datenlord_error *write_file(const char *file_path, datenlord_bytes content);

datenlord_error *read_file(const char *file_path, datenlord_bytes *out_content);

} // extern "C"
