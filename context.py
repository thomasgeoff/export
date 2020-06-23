import os

from kasasa_common.context import DatabaseContext, VaultContext
from kasasa_common.vault import get_vault_secrets
from kasasa_common.logger import logger


def get_db_context(cluster, database):
    db_dict = {
        'DATABASE_HOST': os.getenv(f'{cluster.upper()}_DATABASE_HOST'),
        'DATABASE_PORT': os.getenv(f'{cluster.upper()}_DATABASE_PORT'),
        'DATABASE_NAME': database,
        'DATABASE_USERNAME': os.getenv(f'{cluster.upper()}_DATABASE_USERNAME'),
        'DATABASE_PASSWORD_KEY': os.getenv(f'{cluster.upper()}_DATABASE_PASSWORD_KEY'),
        'VAULT_SECRETS': get_secrets(cluster),
    }
    return DatabaseContext(**db_dict)


def get_secrets(cluster):
    db_password = os.getenv(f'{cluster.upper()}_DATABASE_PASSWORD')
    db_password_key = os.getenv(f'{cluster.upper()}_DATABASE_PASSWORD_KEY')

    if db_password and db_password_key:
        return {db_password_key: db_password}

    vc_kwargs = {
        'VAULT_ADDRESS': os.getenv('VAULT_ADDRESS'),
        'VAULT_APPROLE_PATH': os.getenv('VAULT_APPROLE_PATH'),
        'VAULT_APPROLE_SECRET_ID': os.getenv('VAULT_APPROLE_SECRET_ID'),
        'VAULT_SECRET_KEY': os.getenv('LAMBDA_NAME'),
    }
    vc = VaultContext(**vc_kwargs)
    return get_vault_secrets(vc, logger)
