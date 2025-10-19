from sqlalchemy import (
    Column,
    Date,
    Float,
    Integer,
    String,
    create_engine,
    select,
    delete,
    func,
)
from sqlalchemy.orm import declarative_base, sessionmaker
import json
from pathlib import Path

DATABASE_URL = "sqlite:///db.db"

engine = create_engine(
    DATABASE_URL,
    echo=True,
)

Base = declarative_base()

Session = sessionmaker(bind=engine)


class User(Base):
    __tablename__ = "user"
    user_id = Column(String, primary_key=True, index=True)
    country = Column(String)
    gender = Column(String)

    def dict(self):
        return {
            key: value
            for key, value in self.__dict__.items()
            if not key.startswith("_")
        }


class Event(Base):
    __tablename__ = "event"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, index=True)
    account_type = Column(String)
    date = Column(Date)
    event_type = Column(String)
    order_value = Column(Float)
    version = Column(String)

    def dict(self):
        return {
            key: value
            for key, value in self.__dict__.items()
            if not key.startswith("_")
        }


def init():
    Base.metadata.create_all(bind=engine)
    init_users()


def init_users():
    def _loaded_user_data():
        """Get user data from file"""
        current_file_path = Path(__file__).resolve()
        root_dir = current_file_path.parent.parent
        json_path = root_dir / "users.json"

        user_data = []
        with open(json_path, "r") as f:
            user_data = json.load(f)
        return user_data

    def delete_all_users(session):
        statement = delete(User)
        session.execute(statement)
        session.commit()

    session = Session()
    delete_all_users(session)
    loaded_user_data = _loaded_user_data()
    session.add_all([User(**o) for o in loaded_user_data])
    session.commit()
    session.close()


class Database:
    class DataObject:
        def __init__(self, obj: dict):
            for k, v in obj.items():
                setattr(self, k, v)

    class DataType:
        def __init__(self, session, table_name: str):
            self.table_name = table_name
            self.table = globals().get(table_name.capitalize())
            self.session = session

        def add(self, values: dict):
            self.session.add(self.table(**values))
            self.session.commit()

        def add_all(self, objs: list[dict]):
            self.session.add_all([self.table(**o) for o in objs])
            self.session.commit()

        def get_all(self, as_dict: bool = False):
            statement = select(self.table)
            res = self.session.execute(statement)
            objs = res.scalars().fetchall() or []

            return (
                [o.dict() for o in objs]
                if as_dict
                else [Database.DataObject(do) for do in [o.dict() for o in objs]]
            )

    def __init__(self):
        self.session = Session()
        for o in [self.DataType(self.session, table) for table in Base.metadata.tables]:
            setattr(self, o.table_name, o)

    def get_events_grouped(self, country: str = ""):
        statement = (
            select(
                User.country,
                Event.version,
                func.count(Event.user_id).label("nbr_events"),
            )
            .join(
                User,
                Event.user_id == User.user_id,
            )
            .group_by(
                User.country,
                Event.version,
            )
        )
        if country:
            statement = statement.where(User.country == country)
        return self.session.execute(statement).mappings().all()
