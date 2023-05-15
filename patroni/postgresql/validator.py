import abc
from copy import deepcopy
import logging
import os
import sys
import yaml

from typing import Any, Dict, Iterator, List, MutableMapping, Optional, Tuple, Union, TYPE_CHECKING

from ..collections import CaseInsensitiveDict, CaseInsensitiveSet
from ..utils import parse_bool, parse_int, parse_real

if TYPE_CHECKING:  # pragma: no cover
    from types import ModuleType

logger = logging.getLogger(__name__)


class _Transformable(abc.ABC):

    def __init__(self, version_from: int, version_till: Optional[int]) -> None:
        self.__version_from = version_from
        self.__version_till = version_till

    @property
    def version_from(self) -> int:
        return self.__version_from

    @property
    def version_till(self) -> Optional[int]:
        return self.__version_till

    @abc.abstractmethod
    def transform(self, name: str, value: Any) -> Optional[Any]:
        """Verify that provided value is valid.

        :param name: GUC's name
        :param value: GUC's value
        :returns: the value (sometimes clamped) or ``None`` if the value isn't valid
        """


class Bool(_Transformable):

    def transform(self, name: str, value: Any) -> Optional[Any]:
        if parse_bool(value) is not None:
            return value
        logger.warning('Removing bool parameter=%s from the config due to the invalid value=%s', name, value)


class Number(_Transformable):

    def __init__(self, version_from: int, version_till: Optional[int],
                 min_val: Union[int, float], max_val: Union[int, float], unit: Optional[str]) -> None:
        super(Number, self).__init__(version_from, version_till)
        self.__min_val = min_val
        self.__max_val = max_val
        self.__unit = unit

    @property
    def min_val(self) -> Union[int, float]:
        return self.__min_val

    @property
    def max_val(self) -> Union[int, float]:
        return self.__max_val

    @property
    def unit(self) -> Optional[str]:
        return self.__unit

    @staticmethod
    @abc.abstractmethod
    def parse(value: Any, unit: Optional[str]) -> Optional[Any]:
        """Convert provided value to unit."""

    def transform(self, name: str, value: Any) -> Union[int, float, None]:
        num_value = self.parse(value, self.unit)
        if num_value is not None:
            if num_value < self.min_val:
                logger.warning('Value=%s of parameter=%s is too low, increasing to %s%s',
                               value, name, self.min_val, self.unit or '')
                return self.min_val
            if num_value > self.max_val:
                logger.warning('Value=%s of parameter=%s is too big, decreasing to %s%s',
                               value, name, self.max_val, self.unit or '')
                return self.max_val
            return value
        logger.warning('Removing %s parameter=%s from the config due to the invalid value=%s',
                       self.__class__.__name__.lower(), name, value)


class Integer(Number):

    @staticmethod
    def parse(value: Any, unit: Optional[str]) -> Optional[int]:
        return parse_int(value, unit)


class Real(Number):

    @staticmethod
    def parse(value: Any, unit: Optional[str]) -> Optional[float]:
        return parse_real(value, unit)


class Enum(_Transformable):

    def __init__(self, version_from: int, version_till: Optional[int], possible_values: Tuple[str, ...]) -> None:
        super(Enum, self).__init__(version_from, version_till)
        self.__possible_values = possible_values

    @property
    def possible_values(self) -> Tuple[str, ...]:
        return self.__possible_values

    def transform(self, name: str, value: Optional[Any]) -> Optional[Any]:
        if str(value).lower() in self.possible_values:
            return value
        logger.warning('Removing enum parameter=%s from the config due to the invalid value=%s', name, value)


class EnumBool(Enum):

    def transform(self, name: str, value: Optional[Any]) -> Optional[Any]:
        if parse_bool(value) is not None:
            return value
        return super(EnumBool, self).transform(name, value)


class String(_Transformable):

    def transform(self, name: str, value: Optional[Any]) -> Optional[Any]:
        return value


# Format:
#  key  - parameter name
#  value - tuple of tuples. Each sub-tuple represents a different validation of the GUC across postgres versions. If a
#          GUC validation has never changed over time, then it will have a single sub-tuple. For example,
#          `password_encryption` used to be a boolean GUC up to Postgres 10, at which point it started being an enum.
#          In that case the value of `password_encryption` would be a tuple of 2 tuples, each one reprensenting a
#          different validation rule.
parameters = CaseInsensitiveDict()
recovery_parameters = CaseInsensitiveDict()


def _get_module() -> 'ModuleType':
    """Get a reference to the current module.

    :returns: the module object.
    """
    return sys.modules[__name__]


class ValidatorFactoryNoType(Exception):
    """Raised when a validator spec misses a type."""


class ValidatorFactorInvalidType(Exception):
    """Raised when a validator spec contains an invalid type."""


class ValidatorFactoryInvalidSpec(Exception):
    """Raised when a validator spec contains an invalid set of attributes."""


class ValidatorFactory:
    """Factory class used to build Patroni validator objects based on the given specs."""

    TYPES = ['Bool', 'Integer', 'Real', 'Enum', 'EnumBool', 'String']

    def __new__(cls, validator: Dict[str, Any]) -> Union[Bool, Integer, Real, Enum, EnumBool, String]:
        """Parse a given Postgres GUC *validator* into the corresponding Patroni validator object.

        :param validator: a validator spec for a given parameter. It usually comes from a parsed YAML file.

        :returns: the Patroni validator object that corresponds to the specification found in *validator*.

        :raises :class:`ValidatorFactoryNoType`: if *validator* contains no ``type`` key.
        :raises :class:`ValidatorFactorInvalidType`: if ``type`` key from *validator* contains an invalid value.
        :raises :class:`ValidatorFactoryInvalidSpec`: if *validator* contains an invalid set of attributes for the
            given ``type``.

        :Example:

            If a given validator was defined as follows in the YAML file:

            ```yaml
            - type: String
            version_from: 90300
            version_till: null
            ```

            Then this method would receive *validator* as:

            ```python
            {
                'type': 'String',
                'version_from': 90300,
                'version_till': None
            }
            ```

            And this method would return a :class:`String`:

            ```python
            String(90300, None)
            ```
        """
        validator = deepcopy(validator)
        try:
            type_ = validator.pop('type')
        except KeyError as exc:
            raise ValidatorFactoryNoType('Validator contains no type.') from exc

        if type_ not in cls.TYPES:
            raise ValidatorFactorInvalidType(f'Unexpected validator type: `{type_}`.')

        for key, value in validator.items():
            # :func:`_transform_parameter_value` expects :class:`tuple` instead of :class:`list`
            if isinstance(value, list):
                tmp_value: List[Any] = value
                validator[key] = tuple(tmp_value)

        try:
            return getattr(_get_module(), type_)(**validator)
        except Exception as exc:
            raise ValidatorFactoryInvalidSpec(
                f'Failed to parse `{type_}` validator (`{validator}`): `{str(exc)}`.') from exc


def _load_postgres_guc_validators(section: CaseInsensitiveDict, config: Dict[str, Any], parameter: str) \
        -> Iterator[Union[ValidatorFactoryNoType, ValidatorFactorInvalidType, ValidatorFactoryInvalidSpec]]:
    """Load *parameter* validators from *config* into *section*.

    Loop over all validators of *parameter* and load each of them into *section*.

    :param section: reference to either ``parameters`` or ``recovery_parameters`` variable of this module.
    :param config: Python object corresponding to an YAML file, with values of either ``parameters`` or
        ``recovery_parameters`` key, corresponding to given *section*.
    :param parameter: name of the parameter found under *config* which validators should be parsed and loaded into
        *section*.

    :rtype: yields any exception that is faced while parsing a validator spec into a Patroni validator object.
    """
    for validator_spec in config.get(parameter, {}):
        try:
            validator = ValidatorFactory(validator_spec)
        except (ValidatorFactoryNoType, ValidatorFactorInvalidType, ValidatorFactoryInvalidSpec) as exc:
            yield exc
            continue

        if parameter not in section:
            section[parameter] = ()

        section[parameter] = section[parameter] + (validator,)


class InvalidGucValidatorsFile(Exception):
    """Raised when reading or parsing of a YAML file faces an issue."""


def _read_postgres_gucs_validators_file(file: str) -> Dict[str, Any]:
    """Read an YAML file and return the corresponding Python object.

    :param file: path to the file to be read. It is expected to be encoded with ``UTF-8``, and to be a YAML document.

    :returns: the YAML content parsed into a Python object. If any issue is faced while reading/parsing the file, then
        return ``None``.

    :raises :class:`InvalidGucValidatorsFile`: if faces an issue while reading or parsing *file*.
    """
    try:
        with open(file, encoding='UTF-8') as stream:
            return yaml.safe_load(stream)
    except Exception as exc:
        raise InvalidGucValidatorsFile(
            f'Unexpected issue while reading parameters file `{file}`: `{str(exc)}`.') from exc


def _load_postgres_gucs_validators() -> None:
    """Load all Postgres GUC validators from YAML files.

    Recursively walk through ``available_parameters`` directory and load validators of each found YAML file into
    ``parameters`` and/or ``recovery_parameters`` variables.

    Walk through directories in top-down fashion and for each of them:
        * Sort files by name;
        * Load validators from YAML files that were found.

    Any problem faced while reading or parsing files will be logged as a ``WARNING`` by the child function, and the
    corresponding file or validator will be ignored.

    By default Patroni only ships the file ``0_postgres.yml``, which contains Community Postgres GUCs validators, but
    that behavior can be extended. For example: if a vendor wants to add GUC validators to Patroni for covering a custom
    Postgres build, then they can create their custom YAML files under ``available_parameters`` directory.

    Each YAML file may contain either or both of these root attributes, here called sections:
        * ``parameters``: general GUCs that would be written to ``postgresql.conf``;
        * ``recovery_parameters``: recovery related GUCs that would be written to ``recovery.conf`` (Patroni later
            writes them to ``postgresql.conf`` if running PG 12 and above).

    Then, each of these sections, if specified, may contain one or more attributes with the following structure:
        * key: the name of a GUC;
        * value: a list of validators. Each item in the list must contain a ``type`` attribute, which must be one among:
            * ``Bool``; or
            * ``Integer``; or
            * ``Real``; or
            * ``Enum``; or
            * ``EnumBool``; or
            * ``String``.

            Besides the ``type`` attribute, it should also contain all the required attributes as per the corresponding
            class in this module.

    .. seealso::
        * :class:`Bool`;
        * :class:`Integer`;
        * :class:`Real`;
        * :class:`Enum`;
        * :class:`EnumBool`;
        * :class:`String`.

    :Example:

        This is a sample content for an YAML file based on Postgres GUCs, showing each of the supported types and
        sections:

        ```yaml
        parameters:
          archive_command:
          - type: String
            version_from: 90300
            version_till: null
          archive_mode:
          - type: Bool
            version_from: 90300
            version_till: 90500
          - type: EnumBool
            version_from: 90500
            version_till: null
            possible_values:
            - always
          archive_timeout:
          - type: Integer
            version_from: 90300
            version_till: null
            min_val: 0
            max_val: 1073741823
            unit: s
          autovacuum_vacuum_cost_delay:
          - type: Integer
            version_from: 90300
            version_till: 120000
            min_val: -1
            max_val: 100
            unit: ms
          - type: Real
            version_from: 120000
            version_till: null
            min_val: -1
            max_val: 100
            unit: ms
          client_min_messages:
          - type: Enum
            version_from: 90300
            version_till: null
            possible_values:
            - debug5
            - debug4
            - debug3
            - debug2
            - debug1
            - log
            - notice
            - warning
            - error
        recovery_parameters:
          archive_cleanup_command:
          - type: String
            version_from: 90300
            version_till: null
        ```
    """
    module = _get_module()
    conf_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'available_parameters',
    )
    yaml_files: List[str] = []

    for root, _, files in os.walk(conf_dir):
        for file in sorted(files):
            full_path = os.path.join(root, file)
            if file.lower().endswith(('.yml', '.yaml')):
                yaml_files.append(full_path)
            else:
                logger.info('Ignored a non-YAML file found under `available_parameters` directory: `%s`.', full_path)

    for file in yaml_files:
        try:
            config: Dict[str, Any] = _read_postgres_gucs_validators_file(file)
        except InvalidGucValidatorsFile as exc:
            logger.warning(str(exc))
            continue

        for section in ['parameters', 'recovery_parameters']:
            section_var = getattr(module, section)

            config_section = config.get(section, {})
            for parameter in config_section.keys():
                for exc in _load_postgres_guc_validators(section_var, config_section, parameter):
                    logger.warning('Faced an issue while parsing a validator for parameter `%s`, from file `%s`: `%r`',
                                   parameter, file, exc)


_load_postgres_gucs_validators()


def _transform_parameter_value(validators: MutableMapping[str, Tuple[_Transformable, ...]],
                               version: int, name: str, value: Any,
                               available_gucs: CaseInsensitiveSet) -> Optional[Any]:
    """Validate *value* of GUC *name* for Postgres *version* using defined *validators* and *available_gucs*.

    :param validators: a dictionary of all GUCs across all Postgres versions. Each key is the name of a Postgres GUC,
        and the corresponding value is a tuple of tuples. Each sub-tuple is a validation rule for the GUC for a given
        range of Postgres versions. Should either contain recovery GUCs or general GUCs, not both.
    :param version: Postgres version to validate the GUC against.
    :param name: name of the Postgres GUC.
    :param value: value of the Postgres GUC.
    :param available_gucs: a set of all GUCs available in Postgres *version*. Each item is the name of a Postgres
        GUC. Used for a couple purposes:
        * Disallow writing GUCs to ``postgresql.conf`` (or ``recovery.conf``) that does not exist in Postgres *version*;
        * Avoid ignoring GUC *name* if it does not have a validator in *validators*, but is a valid GUC in Postgres
            *version*.

    :returns: the return value may be one among:
        * *value* transformed to the expected format for GUC *name* in Postgres *version*, if *name* is present in
            *available_gucs* and has a validator in *validators* for the corresponding Postgres *version*; or
        * The own *value* if *name* is present in *available_gucs* but not in *validators*; or
        * ``None`` if *name* is not present in *available_gucs*.
    """
    if name in available_gucs:
        name_validators: Tuple[_Transformable, ...] = validators.get(name, ())
        if name_validators:
            for validator in name_validators:
                if version >= validator.version_from and\
                        (validator.version_till is None or version < validator.version_till):
                    return validator.transform(name, value)
        # Ideally we should have a validator in *validators*. However, if none is available, we will not discard a
        # setting that exists in Postgres *version*, but rather allow the value with no validation.
        return value
    logger.warning('Removing unexpected parameter=%s value=%s from the config', name, value)


def transform_postgresql_parameter_value(version: int, name: str, value: Any,
                                         available_gucs: CaseInsensitiveSet) -> Optional[Any]:
    """Validate *value* of GUC *name* for Postgres *version* using ``parameters`` and *available_gucs*.

    :param version: Postgres version to validate the GUC against.
    :param name: name of the Postgres GUC.
    :param value: value of the Postgres GUC.
    :param available_gucs: a set of all GUCs available in Postgres *version*. Each item is the name of a Postgres
        GUC. Used for a couple purposes:
        * Disallow writing GUCs to ``postgresql.conf`` that does not exist in Postgres *version*;
        * Avoid ignoring GUC *name* if it does not have a validator in ``parameters``, but is a valid GUC in Postgres
            *version*.

    :returns: The return value may be one among
        * The original *value* if *name* seems to be an extension GUC (contains a period '.'); or
        * ``None`` if **name** is a recovery GUC; or
        * *value* transformed to the expected format for GUC *name* in Postgres *version* using validators defined in
            ``parameters``. Can also return ``None``. See :func:`_transform_parameter_value`.
    """
    if '.' in name:
        if name not in parameters:
            # likely an extension GUC, so just return as it is. Otherwise, if `name` is in `parameters`, it's likely a
            # namespaced GUC from a custom Postgres build, so we treat that over the usual validation means.
            return value
    if name in recovery_parameters:
        return None
    return _transform_parameter_value(parameters, version, name, value, available_gucs)


def transform_recovery_parameter_value(version: int, name: str, value: Any,
                                       available_gucs: CaseInsensitiveSet) -> Optional[Any]:
    """Validate *value* of GUC *name* for Postgres *version* using ``recovery_parameters`` and *available_gucs*.

    :param version: Postgres version to validate the recovery GUC against.
    :param name: name of the Postgres recovery GUC.
    :param value: value of the Postgres recovery GUC.
    :param available_gucs: a set of all GUCs available in Postgres *version*. Each item is the name of a Postgres
        GUC. Used for a couple purposes:
        * Disallow writing GUCs to ``recovery.conf`` (or ``postgresql.conf`` depending on *version*), that does not
        exist in Postgres *version*;
        * Avoid ignoring recovery GUC *name* if it does not have a validator in ``recovery_parameters``, but is a valid
            GUC in Postgres *version*.

    :returns: *value* transformed to the expected format for recovery GUC *name* in Postgres *version* using validators
        defined in ``recovery_parameters``. It can also return ``None``. See :func:`_transform_parameter_value`.
    """
    return _transform_parameter_value(recovery_parameters, version, name, value, available_gucs)
