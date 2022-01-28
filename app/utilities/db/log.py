# db
from os.path import isfile
from sqlite3 import connect
import asyncio
import asqlite
import shutil

# custom utilities and setup
from Utilities import Log

DB_PATH = "./app/data/database.db"
BUILD_PATH = "build.sql"

class Database:
    def __init__(self, module):
        self.module = f"{module}.db"
        self.log = Log.Logger(name=self.module)

    async def backup(self):
        shutil.copy2("./Data/database.db", "./Data/database.db.bak")
        await self.log.info("Backed up database.")

    async def build(self):
        async with asqlite.connect(DB_PATH) as connection:
            async with connection.cursor() as cursor:
                if isfile(BUILD_PATH):
                    with open(BUILD_PATH, "r", encoding="utf-8") as script:
                        await cursor.executescript(script.read())

                    await self.log.info("Database built")

    async def commit(self):
        async with asqlite.connect(DB_PATH) as connection:
            await connection.commit()

            await self.log.info("Committed to database.")

    async def close(self):
        async with asqlite.connect(DB_PATH) as connection:
            await connection.close()

            await self.log.info("Closed database connection.")

    async def field(self, command, *values):
        async with asqlite.connect(DB_PATH) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(command, tuple(values))
                
                await self.log.info(f"Executed {command} with {values}.")

                if (fetch := await cursor.fetchone()) is not None: 
                    await log.info(f"Fetched {fetch}.")
                    return fetch[0]

    async def record(self, command, *values):
        async with asqlite.connect(DB_PATH) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(command, tuple(values))

                await self.log.info(f"Executed {command} with {values}.")
                
                return await cursor.fetchone()

    async def records(self, command, *values):
        async with asqlite.connect(DB_PATH) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(command, tuple(values))
                await self.log.info(f"Executed {command} with {values}.")

                return await cursor.fetchall()

    async def column(self, command, *values):
        async with asqlite.connect(DB_PATH) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(command, tuple(values))

                await self.log.info(f"Executed {command} with {values}.")

                return [item[0] for item in await cursor.fetchall()]
    # args and kwargs for values
    async def execute(self, command, *values):
        async with asqlite.connect(DB_PATH) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(command, tuple(values))

                await self.log.info(f"Executed {command} with {values}.")

    async def multiexec(self, command, valueset):
        async with asqlite.connect(DB_PATH) as connection:
            async with connection.cursor() as cursor:
                await cursor.executemany(command, valueset)

                await self.log.info(f"Executed {command} with {valueset}.")
