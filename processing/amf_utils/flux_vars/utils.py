import re
from dataclasses import dataclass
from typing import Union


@dataclass
class BestMatch:
    key: str
    match_str: str


@dataclass
class VarAttrs:
    base_var: str = None
    has_QC_flag: bool = False
    has_inst_units: bool = False
    is_aggregate: bool = False
    is_gap_filled: bool = False
    is_invalid: bool = False
    is_number_of_samples: bool = False
    is_PI_provided: bool = False
    is_standard_deviation: bool = False

    aggregation_layer_index: int = None
    horizontal_index: int = None
    replicate_index: int = None
    vertical_index: int = None


class VarUtils:
    def __init__(self, known_variables_without_qualifiers: tuple = None):
        _pattern_template = {
            'h_v_r': '^[0-9]+_[0-9]+_[0-9]+$',
            'h_v_A': '^[0-9]+_[0-9]+_A$',
            'h_v_A_SD': '^[0-9]+_[0-9]+_A_SD$',
            'h_v_A_N': '^[0-9]+_[0-9]+_A_N$',
            '#': '^[0-9]+$',
            '#_SD': '^[0-9]+_SD$',
            '#_N': '^[0-9]+_N$',
        }

        self.valid_patterns = {}
        for k, p in _pattern_template.items():
            self.valid_patterns[k] = re.compile(p)

        self.known_variables_without_qualifiers = tuple()
        if known_variables_without_qualifiers:
            self.known_variables_without_qualifiers = \
                known_variables_without_qualifiers
        self.fp_in_general_qualifiers = ('PI', 'QC', 'F', 'IU')

    def _get_var_base(self, var: str):
        best_match = None
        for known_variable in self.known_variables_without_qualifiers:
            if not var.startswith(known_variable):
                continue
            if not best_match:
                best_match = BestMatch(None, known_variable)
            elif len(known_variable) > len(best_match.match_str):
                best_match.match_str = known_variable

        if best_match:
            return best_match.match_str
        if self.known_variables_without_qualifiers:
            return False
        return None

    def _get_non_general_qualifiers_only(self, var):
        if not var:
            return

        var_base = self._get_var_base(var)
        if var_base:
            var = var[len(var_base):]

        var_chunks = var.split('_')
        for idx, v in enumerate(var_chunks):
            if v == '' or (v.isalnum() and not v.isnumeric()):
                continue
            else:
                return '_'.join(var_chunks[idx:])

    def _gen_var_attr(self, var, best_match, no_general_qualifiers=False):
        base_var_chunks = []
        is_potential_base_var = True
        var_attrs = VarAttrs()

        if not var or any(
                (var.startswith('_'), var.endswith('_'), '__' in var)):
            var_attrs.is_invalid = True
            return var_attrs

        var_base = self._get_var_base(var)
        if var_base:
            var = var.replace(var_base, '', 1)
        if var_base is False:
            var_attrs.is_invalid = True
            return var_attrs

        var_chunks = var.split('_')

        if not best_match and not no_general_qualifiers:
            var_attrs.is_invalid = True
            return var_attrs

        for v in var_chunks:
            v = v.upper()
            if v in self.fp_in_general_qualifiers:
                is_potential_base_var = False
                if v == 'PI':
                    var_attrs.is_PI_provided = True
                elif v == 'QC':
                    var_attrs.has_QC_flag = True
                elif v == 'F':
                    var_attrs.is_gap_filled = True
                elif v == 'IU':
                    var_attrs.has_inst_units = True

            elif v.isdecimal():
                break
            elif is_potential_base_var and v:
                base_var_chunks.append(v)

        if best_match:
            key_chunks = best_match.key.split('_')
            non_general_qualifier_chunks = best_match.match_str.split('_')
            for k, c in zip(key_chunks, non_general_qualifier_chunks):
                if k == 'h':
                    var_attrs.horizontal_index = int(c)
                elif k == 'v':
                    var_attrs.vertical_index = int(c)
                elif k == 'r':
                    var_attrs.replicate_index = int(c)
                elif k == '#':
                    var_attrs.aggregation_layer_index = int(c)
                elif k == 'SD':
                    var_attrs.is_standard_deviation = True
                elif k == 'N':
                    var_attrs.is_number_of_samples = True
                elif k == 'A':
                    var_attrs.is_aggregate = True

        if var_base:
            var_attrs.base_var = var_base

        elif base_var_chunks:
            var_attrs.base_var = '_'.join(base_var_chunks)

        if self.known_variables_without_qualifiers \
                and var_base \
                and base_var_chunks:
            var_attrs.is_invalid = True

        return var_attrs

    def parse_var(self, var: str) -> VarAttrs:
        best_match = None
        qualifiers_only = self._get_non_general_qualifiers_only(var)
        if not qualifiers_only:
            return self._gen_var_attr(
                var, best_match, no_general_qualifiers=True)

        for key, pattern in self.valid_patterns.items():
            results = pattern.match(qualifiers_only)
            if not results:
                continue

            match_str = results.group(0)
            if not best_match:
                best_match = BestMatch(key, match_str)
            elif len(match_str) < len(best_match.match_str):
                continue
            best_match.key = key
            best_match.match_str = match_str

        return self._gen_var_attr(var, best_match)

    def tag_PI_for_BASE_var(
            self, var: str,
            PI_vars=('GPP', 'NEE', 'RECO', 'VPD')) -> Union[str, None]:
        var_attr = self.parse_var(var)
        if var_attr.is_invalid:
            return None

        # Ordering of qualifiers as follows:
        # 1. General qualifiers in order as self.fp_in_general_qualifiers
        # 2. Positional Qualifiers [H_V_R] or
        # 3. Aggregation qualifiers [H_V_A, #, SD, N]

        new_base_var_components = [var_attr.base_var]
        attr_tag_map = {
            'PI': (var_attr.is_PI_provided
                   or var_attr.base_var in PI_vars
                   or var_attr.is_gap_filled
                   or var_attr.is_aggregate
                   or var_attr.aggregation_layer_index is not None),
            'QC': var_attr.has_QC_flag,
            'F': var_attr.is_gap_filled,
            'IU': var_attr.has_inst_units,
            'H': var_attr.horizontal_index,
            'V': var_attr.vertical_index,
            'R': var_attr.replicate_index,
            'A': var_attr.is_aggregate,
            '#': var_attr.aggregation_layer_index,
            'SD': var_attr.is_standard_deviation,
            'N': var_attr.is_number_of_samples}

        for tag, v in attr_tag_map.items():
            if v:
                if tag in ('H', 'V', 'R', '#'):
                    tag = f'{v}'
                new_base_var_components.append(tag)

        return '_'.join(new_base_var_components)
