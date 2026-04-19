from core.state import app_state


def get_active_operation(user_id: int) -> str | None:
    return app_state.active_operations.get(user_id)


def try_begin_operation(user_id: int, operation_name: str) -> bool:
    active = app_state.active_operations.get(user_id)
    if active and active != operation_name:
        return False
    app_state.active_operations[user_id] = operation_name
    return True


def end_operation(user_id: int, operation_name: str | None = None) -> None:
    active = app_state.active_operations.get(user_id)
    if operation_name is None or active == operation_name:
        app_state.active_operations.pop(user_id, None)
