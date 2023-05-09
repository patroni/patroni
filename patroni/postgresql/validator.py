import abc
import logging
import os
import sys
import yaml

from typing import Any, List, MutableMapping, Optional, Tuple, Union

from ..collections import CaseInsensitiveDict
from ..utils import parse_bool, parse_int, parse_real

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


def _load_postgres_gucs_validators():
    """Load all Postgres GUC validators from YAML files.

    Recursively walk through ``available_parameters`` directory and load validators of each found YAML file into
    ``parameters`` and/or ``recovery_parameters`` variables.

    Walk through directories in top-down fashion and for each of them:
        * Sort files by name;
        * Load validators from YAML files that were found.

    Any problem faced while reading or parsing files will be logged as a ``WARNING``, and the corresponding file or
    validator will be ignored.

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
    module = sys.modules[__name__]

    conf_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'available_parameters',
    )

    for root, _, files in os.walk(conf_dir):
        for file in sorted(files):
            if not file.lower().endswith(('.yml', '.yaml')):
                continue

            full_path = os.path.join(root, file)
            try:
                with open(full_path) as f:
                    config = yaml.safe_load(f)
            except Exception as e:
                logger.warning('Unexpected issue while reading parameters file `{0}`: `{1}`.'.format(full_path, str(e)))
                continue

            for section in ['parameters', 'recovery_parameters']:
                section_var = getattr(module, section)

                for parameter, validators in config.get(section, {}).items():
                    for validator in validators:
                        try:
                            type = validator.pop('type')
                        except KeyError:
                            logger.warning(
                                'Validator for parameter `{0}` in file `{1}` contains no type.'.format(
                                    parameter, full_path
                                )
                            )
                            continue

                        if type not in ['Bool', 'Integer', 'Real', 'Enum', 'EnumBool', 'String']:
                            logger.warning(
                                'Unexpected validator type for parameter `{0}` in file `{1}`: `{2}`.'.format(
                                    parameter, full_path, type
                                )
                            )
                            continue

                        for key, value in validator.items():
                            # :func:`_transform_parameter_value` expects :class:`tuple` instead of :class:`list`
                            if isinstance(value, list):
                                tmp_value: List[Any] = value
                                validator[key] = tuple(tmp_value)

                        try:
                            validator_obj = getattr(module, type)(**validator)
                        except Exception as e:
                            logger.warning(
                                'Failed to parse `{0}` validator for parameter `{1}` (`{2}`) '
                                'from file {3}: `{4}`.'.format(
                                    type, parameter, full_path, validator, str(e)
                                )
                            )
                            continue

                        if parameter not in section_var:
                            section_var[parameter] = ()

                        section_var[parameter] = section_var[parameter] + (validator_obj,)


_load_postgres_gucs_validators()


def _transform_parameter_value(validators: MutableMapping[str, Union[_Transformable, Tuple[_Transformable, ...]]],
                               version: int, name: str, value: Any) -> Optional[Any]:
    name_validators = validators.get(name)
    if name_validators:
        for validator in (name_validators if isinstance(name_validators, tuple) else (name_validators,)):
            if version >= validator.version_from and\
                    (validator.version_till is None or version < validator.version_till):
                return validator.transform(name, value)
    logger.warning('Removing unexpected parameter=%s value=%s from the config', name, value)


def transform_postgresql_parameter_value(version: int, name: str, value: Any) -> Optional[Any]:
    if '.' in name:
        return value
    if name in recovery_parameters:
        return None
    return _transform_parameter_value(parameters, version, name, value)


def transform_recovery_parameter_value(version: int, name: str, value: Any) -> Optional[Any]:
    return _transform_parameter_value(recovery_parameters, version, name, value)
