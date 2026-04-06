from database import get_all_vip_users


vip_users_cache = set()


async def update_vip_cache():
    """Обновить кэш VIP списка."""
    global vip_users_cache
    vip_users_cache = set(get_all_vip_users())


def is_vip_user_cached(user_id: int) -> bool:
    """Быстрая проверка VIP статуса через кэш (вместо запроса в БД)."""
    return user_id in vip_users_cache
