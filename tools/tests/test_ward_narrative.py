"""Ward narratives (evaluation P7): wards are a narrative level, and the subject
line names the parent municipality instead of echoing the raw display name."""
from tools.narrative import generate_narratives as gn
from tools.narrative.templates import LEVEL_CONFIGS, _format_jurisdiction_subject


def _subject(name):
    return _format_jurisdiction_subject({'name': name, 'level': 'ward'},
                                        LEVEL_CONFIGS['ward'])


def test_ward_is_a_non_county_level():
    assert 'ward' in gn.NON_COUNTY_LEVELS


def test_city_ward_subject():
    assert _subject('DAYTON WARD 3 (DAYTON City)') == 'Dayton Ward 3 (City of Dayton)'


def test_village_ward_and_ordinal_casing():
    # str.title() would give '1St Ward'.
    assert _subject('1ST WARD (NEW LEBANON Village)') == '1st Ward (Village of New Lebanon)'


def test_unrecognised_shape_falls_back_to_generic():
    assert _subject('ODD NAME') == 'Odd Name ward'
