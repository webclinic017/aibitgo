#! bin/bash
cd db && alembic revision --autogenerate -m "init" && alembic upgrade head
