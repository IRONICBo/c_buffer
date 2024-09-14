import datenlord

# Utils functions to handle errors
def handle_error(err):
    if err is not None:
        print(f"Error code: {err['code']}, message: {err['message'].decode()}")
        # Since in Python memory is managed by Python, no need for manual free

def main():
    # Init SDK
    sdk = datenlord.init("example_config")
    if sdk is None:
        print("Failed to initialize SDK")
        return
    print("SDK initialized successfully")

    # Check if directory exists
    dir_exists = datenlord.exists(sdk, "/datenlord_sdk")
    print(f"Directory exists: {dir_exists}")

    # Mkdir /example_dir
    err = datenlord.mkdir(sdk, "example_dir/")
    if err is None:
        print("Directory created successfully")
    else:
        handle_error(err)

    # Create file
    err = datenlord.create_file(sdk, "/example_dir/example_file.txt")
    if err is None:
        print("File created successfully")
    else:
        handle_error(err)

    # Write file
    file_path = "/example_dir/example_file.txt"
    file_content = "Hello, Datenlord!"
    content = memoryview(file_content.encode())  # Using Python memoryview for buffer
    err = datenlord.write_file(sdk, file_path, content)
    if err is None:
        print("File written successfully")
    else:
        handle_error(err)

    # Read file
    err, read_content = datenlord.read_file(sdk, file_path)
    if err is None:
        print(f"File read successfully: {read_content.decode()}")
    else:
        handle_error(err)

    # Stat file
    err, file_stat = datenlord.stat(sdk, "/example_dir/example_file.txt")
    if err is None:
        print(f"File stat: {file_stat}")
    else:
        handle_error(err)

    # Rename file
    err = datenlord.rename_path(sdk, "/example_dir/example_file.txt", "/example_dir/renamed_file.txt")
    if err is None:
        print("File renamed successfully")
    else:
        handle_error(err)

    # Delete directory
    err = datenlord.deldir(sdk, "/example_dir", True)
    if err is None:
        print("Directory deleted successfully")
    else:
        handle_error(err)

    # Release SDK
    datenlord.free_sdk(sdk)
    print("SDK released successfully")


if __name__ == "__main__":
    main()
