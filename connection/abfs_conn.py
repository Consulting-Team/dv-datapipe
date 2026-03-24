from config import config, logger
from adlfs import AzureBlobFileSystem

BASE = f"{config.hs4v1_abfs_strg_cont}/user/hive/"


def _initialize() -> AzureBlobFileSystem:
    fs = AzureBlobFileSystem(
        account_name=config.hs4v1_abfs_strg_acc, account_key=config.hs4v1_abfs_strg_key
    )

    logger.debug("Successfully connected to Azure Blob.")

    # ------------------------------------------------------------
    # paths = fs.ls(BASE)
    # for path in paths:
    #     if fs.isdir(path):
    #         print(path)
    # ------------------------------------------------------------

    return fs


abfs: AzureBlobFileSystem = _initialize()
