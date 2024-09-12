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

template<typename T = void>
struct Arc;

struct LocalFS;

struct datenlord_sdk {
  Arc<Mutex<LocalFS>> localfs;
};

struct datenlord_bytes {
  const uint8_t *data;
  uintptr_t len;
};

struct datenlord_error {
  unsigned int code;
  datenlord_bytes message;
};

extern "C" {

datenlord_sdk *init(const char *config);

void free_sdk(datenlord_sdk *sdk);

bool exists(datenlord_sdk *sdk, const char *dir_path);

datenlord_error *mkdir(datenlord_sdk *sdk, const char *dir_path);

datenlord_error *delete_dir(datenlord_sdk *sdk, const char *dir_path, bool recursive);

datenlord_error *rename_path(datenlord_sdk *sdk, const char *src_path, const char *dest_path);

datenlord_error *copy_from_local_file(datenlord_sdk *sdk,
                                      bool overwrite,
                                      const char *local_file_path,
                                      const char *dest_file_path);

datenlord_error *copy_to_local_file(datenlord_sdk *sdk,
                                    const char *src_file_path,
                                    const char *local_file_path);

datenlord_error *stat(datenlord_sdk *sdk, const char *file_path);

datenlord_error *write_file(datenlord_sdk *sdk, const char *file_path, datenlord_bytes content);

datenlord_error *read_file(datenlord_sdk *sdk, const char *file_path, datenlord_bytes *out_content);

} // extern "C"
