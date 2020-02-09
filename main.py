import argparse
import logging
from datetime import datetime, timedelta, timezone

import sqlalchemy
from sqlalchemy import Column, Integer, Float, types, orm
from sqlalchemy.ext.declarative import declarative_base

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


def now() -> int:
    return int(datetime.timestamp(datetime.now(tz=timezone.utc)))


class Timestamp(types.TypeDecorator):
    impl = types.Integer

    def process_bind_param(self, value: datetime, dialect) -> int:
        return int(value.timestamp())

    def process_result_value(self, value, dialect) -> datetime:
        return datetime.fromtimestamp(value, tz=timezone.utc)


Base = declarative_base()


class Point(Base):
    __tablename__ = "points"

    id = Column(Integer, primary_key=True)
    time = Column(Timestamp, default=now)
    temperature = Column(Float)

    def __repr__(self):
        return "<User(id='{}', time='{}', temperature='{}')>".format(
            self.id, self.time, self.temperature
        )


def generate(session: orm.Session, n_points: int):
    start_time = datetime.now(tz=timezone.utc)
    points = []
    log.info("Start to generate points")
    for t in range(n_points):
        points.append(Point(temperature=20.5, time=(start_time + timedelta(seconds=t))))

    log.info("Add all points to the session")
    session.add_all(points)
    log.info("Start commit")
    session.commit()
    log.info("End commit")


def query(session: orm.Session):
    first = session.query(Point).first()
    last = session.query(Point).order_by(Point.time.desc()).first()
    log.info("First : %s ; Last : %s", first, last)

    duration = (last.time - first.time).total_seconds()
    log.info("Total duration : %s", duration)
    log.info("Total number of points: %s", session.query(Point).count())

    selection = session.query(Point).filter(
        Point.time.between(
            first.time + timedelta(seconds=duration / 3),
            last.time - timedelta(seconds=duration / 3),
        )
    )

    log.info("Number of selected points : %s", selection.count())
    log.info("First selected : %s", selection.first())


def init(filename: str) -> orm.Session:
    engine = sqlalchemy.create_engine(f"sqlite:///{filename}", echo=False)
    Base.metadata.create_all(engine)

    Session = orm.sessionmaker(bind=engine)
    return Session()


def exec_generate(args):
    generate(session=init(args.filename), n_points=args.number_of_points)


def exec_query(args):
    query(session=init(args.filename))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    generate_parser = subparsers.add_parser("generate")
    generate_parser.add_argument("--number-of-points", type=int, default=1000)
    generate_parser.add_argument("--filename", type=str, default="foo.db")
    generate_parser.set_defaults(func=exec_generate)

    generate_parser = subparsers.add_parser("query")
    generate_parser.set_defaults(func=exec_query)
    generate_parser.add_argument("--filename", type=str, default="foo.db")

    args = parser.parse_args()
    args.func(args)
