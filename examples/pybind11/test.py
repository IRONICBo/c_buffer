import datenlord

sdk = datenlord.init("config_string")
result = datenlord.exists(sdk, "/some/directory")

print(f"Directory exists: {result}")

datenlord.free_sdk(sdk)
