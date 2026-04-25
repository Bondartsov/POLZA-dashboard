#!/usr/bin/env python3
from config import get_session, ApiKey

session = get_session()
keys = session.query(ApiKey.name).filter(ApiKey.name.isnot(None), ApiKey.name != "").all()
employees = [k[0] for k in keys]
print(f"Total employees: {len(employees)}\n")
for i, name in enumerate(sorted(employees), 1):
    print(f"{i}. {name}")
session.close()
