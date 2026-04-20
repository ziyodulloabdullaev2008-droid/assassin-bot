from database import get_all_vip_users, is_vip_user


vip_users_cache = set()


async def update_vip_cache():
    """Обновить кэш VIP-списка."""
    vip_users_cache.clear()
    vip_users_cache.update(get_all_vip_users())


def add_vip_user_to_cache(user_id: int):
    """Добавить пользователя в VIP-кэш без полной перезагрузки списка."""
    vip_users_cache.add(user_id)


def remove_vip_user_from_cache(user_id: int):
    """Удалить пользователя из VIP-кэша без полной перезагрузки списка."""
    vip_users_cache.discard(user_id)


def get_vip_cache_size() -> int:
    return len(vip_users_cache)


def is_vip_user_cached(user_id: int) -> bool:
    """Check VIP status and keep the in-memory cache in sync."""
    if is_vip_user(user_id):
        vip_users_cache.add(user_id)
        return True

    vip_users_cache.discard(user_id)
    return False
