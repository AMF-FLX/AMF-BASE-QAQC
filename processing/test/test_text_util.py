import pytest
from utils import TextUtil

__author__ = 'Sy-Toan Ngo'
__email__ = 'sytoanngo@lbl.gov'


@pytest.fixture
def text_util():
    ''' Initializes TextUtil '''
    return TextUtil()


def test_strip_whitespace(text_util):
    test_and_comparison = []

    test_tokens = (
        '201601010000, 201601010030, -63.3900, -37.3606, -6.1844, '
        '-14.8509, 0.1838, 0.1112, 408.7241, 7.2133, 5.7312, 86.3784, '
        '3.1180, 80.5577, 2.1530,  80.7000, 5.2200, 74.3869, 2.3016, '
        '82.3000, 0.0000, 0.0000, 0.0000, 266.5000, 325.9000, 2.0850, '
        '0.1039, 0.2195, 0.1039, 2.0000, 7.0000, 9.0000, 4.5000, 8.2000, '
        '9.3000').split(',')
    result_tokens = (
        '201601010000,201601010030,-63.3900,-37.3606,-6.1844,'
        '-14.8509,0.1838,0.1112,408.7241,7.2133,5.7312,86.3784,'
        '3.1180,80.5577,2.1530,80.7000,5.2200,74.3869,2.3016,'
        '82.3000,0.0000,0.0000,0.0000,266.5000,325.9000,2.0850,'
        '0.1039,0.2195,0.1039,2.0000,7.0000,9.0000,4.5000,8.2000,'
        '9.3000').split(',')
    test_and_comparison.append((test_tokens, result_tokens, True))

    tokens = ('201601010000,201601010030,-63.3900,-37.3606,-6.1844').split(',')
    test_and_comparison.append((tokens, tokens, False))

    test_tokens = (' 201601010000,201601010030 ').split(',')
    result_tokens = ('201601010000,201601010030').split(',')
    test_and_comparison.append((test_tokens, result_tokens, True))

    for test_tokens, result_tokens, result_boolean in test_and_comparison:
        assert text_util.strip_whitespace(test_tokens) == \
            (result_tokens, result_boolean)


def test_strip_quotes(text_util):
    test_and_comparison = []

    test_tokens = (
        '"201601010000","201601010030","-63.3900","-37.3606","-6.1844",'
        '"-14.8509","0.1838","0.1112","408.7241","7.2133","5.7312","86.3784",'
        '"3.1180","80.5577","2.1530","80.7000","5.2200","74.3869","2.3016",'
        '"9.3000"').split(',')
    result_tokens = (
        '201601010000,201601010030,-63.3900,-37.3606,-6.1844,'
        '-14.8509,0.1838,0.1112,408.7241,7.2133,5.7312,86.3784,'
        '3.1180,80.5577,2.1530,80.7000,5.2200,74.3869,2.3016,'
        '9.3000').split(',')
    test_and_comparison.append((test_tokens, result_tokens, True))

    tokens = ('201601010000,201601010030,-63.3900,-37.3606,-6.1844').split(',')
    test_and_comparison.append((tokens, tokens, False))

    test_tokens = ('"201601010000,2016010"10030').split(',')
    result_tokens = ('201601010000,2016010"10030').split(',')
    test_and_comparison.append((test_tokens, result_tokens, True))

    for test_tokens, result_tokens, result_boolean in test_and_comparison:
        assert text_util.strip_quotes(test_tokens) == \
            (result_tokens, result_boolean)
