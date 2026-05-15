# Copyright: See the LICENSE file.

import inspect

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound

from . import base, enums, errors

SESSION_PERSISTENCE_COMMIT = 'commit'
SESSION_PERSISTENCE_FLUSH = 'flush'
VALID_SESSION_PERSISTENCE_TYPES = [
    None,
    SESSION_PERSISTENCE_COMMIT,
    SESSION_PERSISTENCE_FLUSH,
]


class SQLAlchemyOptions(base.FactoryOptions):
    def _check_sqlalchemy_session_persistence(self, meta, value):
        if value not in VALID_SESSION_PERSISTENCE_TYPES:
            raise TypeError(
                "%s.sqlalchemy_session_persistence must be one of %s, got %r" %
                (meta, VALID_SESSION_PERSISTENCE_TYPES, value)
            )

    def _build_default_options(self):
        return super()._build_default_options() + [
            base.OptionDefault('sqlalchemy_get_or_create', (), inherit=True),
            base.OptionDefault('sqlalchemy_session', None, inherit=True),
            base.OptionDefault(
                'sqlalchemy_session_persistence',
                None,
                inherit=True,
                checker=self._check_sqlalchemy_session_persistence,
            ),
        ]


class SQLAlchemyModelFactory(base.Factory):
    """Factory for SQLAlchemy models. """

    _options_class = SQLAlchemyOptions

    class Meta:
        abstract = True

    @classmethod
    def _generate(cls, strategy, params):
        # Original params are used in _get_or_create if it cannot build an
        # object initially due to an IntegrityError being raised
        cls._original_params = params
        return super()._generate(strategy, params)

    @classmethod
    def _get_or_create(cls, model_class, session, args, kwargs):
        key_fields = {}
        for field in cls._meta.sqlalchemy_get_or_create:
            if field not in kwargs:
                raise errors.FactoryError(
                    "sqlalchemy_get_or_create - "
                    "Unable to find initialization value for '%s' in factory %s" %
                    (field, cls.__name__))
            key_fields[field] = kwargs.pop(field)

        obj = session.query(model_class).filter_by(
            *args, **key_fields).one_or_none()

        if not obj:
            try:
                obj = cls._save(model_class, session, args, {**key_fields, **kwargs})
            except IntegrityError as e:
                session.rollback()
                get_or_create_params = {
                    lookup: value
                    for lookup, value in cls._original_params.items()
                    if lookup in cls._meta.sqlalchemy_get_or_create
                }
                if get_or_create_params:
                    try:
                        obj = session.query(model_class).filter_by(
                            **get_or_create_params).one()
                    except NoResultFound:
                        # Original params are not a valid lookup and triggered a create(),
                        # that resulted in an IntegrityError.
                        raise e
                else:
                    raise e

        return obj

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        """Create an instance of the model, and save it to the database."""
        session = cls._meta.sqlalchemy_session

        if session is None:
            raise RuntimeError("No session provided.")
        if cls._meta.sqlalchemy_get_or_create:
            return cls._get_or_create(model_class, session, args, kwargs)
        return cls._save(model_class, session, args, kwargs)

    @classmethod
    def _save(cls, model_class, session, args, kwargs):
        session_persistence = cls._meta.sqlalchemy_session_persistence

        obj = model_class(*args, **kwargs)
        session.add(obj)
        if session_persistence == SESSION_PERSISTENCE_FLUSH:
            session.flush()
        elif session_persistence == SESSION_PERSISTENCE_COMMIT:
            session.commit()
        return obj


class AsyncSQLAlchemyModelFactory(base.Factory):
    """Factory for SQLAlchemy models using AsyncSession.

    All create operations are async — use ``await Factory.create()``
    instead of ``Factory.create()``.

    Requires ``Meta.sqlalchemy_session`` to be an ``AsyncSession``.

    Example::

        class WorkspaceFactory(AsyncSQLAlchemyModelFactory):
            class Meta:
                model = Workspace
                sqlalchemy_session = my_async_session

            name = factory.Faker("company")

        workspace = await WorkspaceFactory.create(name="Acme")
    """

    _options_class = SQLAlchemyOptions

    class Meta:
        abstract = True

    @classmethod
    async def _agenerate(cls, strategy, params):
        cls._original_params = params
        return await super()._agenerate(strategy, params)

    @classmethod
    async def _get_or_create(cls, model_class, session, args, kwargs):
        key_fields = {}
        for field in cls._meta.sqlalchemy_get_or_create:
            if field not in kwargs:
                raise errors.FactoryError(
                    "sqlalchemy_get_or_create - "
                    "Unable to find initialization value for '%s' in factory %s" %
                    (field, cls.__name__))
            key_fields[field] = kwargs.pop(field)

        stmt = select(model_class).filter_by(*args, **key_fields)
        result = await session.execute(stmt)
        obj = result.scalars().one_or_none()

        if not obj:
            try:
                obj = await cls._save(model_class, session, args, {**key_fields, **kwargs})
            except IntegrityError as e:
                await session.rollback()
                get_or_create_params = {
                    lookup: value
                    for lookup, value in cls._original_params.items()
                    if lookup in cls._meta.sqlalchemy_get_or_create
                }
                if get_or_create_params:
                    try:
                        retry_stmt = select(model_class).filter_by(**get_or_create_params)
                        retry_result = await session.execute(retry_stmt)
                        obj = retry_result.scalars().one()
                    except NoResultFound:
                        raise e
                else:
                    raise e

        return obj

    @classmethod
    async def _create(cls, model_class, *args, **kwargs):
        """Create an instance of the model, and save it to the database (async)."""
        session = cls._meta.sqlalchemy_session

        if session is None:
            raise RuntimeError("No session provided.")
        if cls._meta.sqlalchemy_get_or_create:
            return await cls._get_or_create(model_class, session, args, kwargs)
        return await cls._save(model_class, session, args, kwargs)

    @classmethod
    async def _save(cls, model_class, session, args, kwargs):
        session_persistence = cls._meta.sqlalchemy_session_persistence

        obj = model_class(*args, **kwargs)
        session.add(obj)
        if session_persistence == SESSION_PERSISTENCE_FLUSH:
            await session.flush()
        elif session_persistence == SESSION_PERSISTENCE_COMMIT:
            await session.commit()
        return obj

    @classmethod
    async def _after_postgeneration(cls, instance, create, results=None):
        """Async hook called after post-generation declarations have been handled."""
        pass

    @classmethod
    async def create(cls, **kwargs):
        """Create an instance of the associated class, with overridden attrs."""
        return await cls._agenerate(enums.CREATE_STRATEGY, kwargs)

    @classmethod
    async def create_batch(cls, size, **kwargs):
        """Create a batch of instances of the given class, with overridden attrs."""
        return [await cls.create(**kwargs) for _ in range(size)]
