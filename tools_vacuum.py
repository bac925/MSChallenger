import asyncio
from db import vacuum_db

asyncio.run(vacuum_db())
print("VACUUM done")
