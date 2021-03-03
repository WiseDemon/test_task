"""
Function for matching strings to patterns
redis style
"""
from src.exceptions.storage_exceptions import StoragePatternError


def str_match_pattern_redis(s: str, pattern: str) -> int:
    """
    Check if a string matches a pattern redis style:
    ? - any arbitrary character,
    * - any number of any characters,
    [ae] - one 'a' or 'e' character,
    [^ea] - one not 'e' or 'a' character,
    [a-c] - one character from 'a' to 'c'.
    :param s: string to check
    :param pattern:
    :return: Position in the string where it stops
    to match pattern or -1 if the string matches
    """
    pat_pos = 0
    s_pos = 0
    while pat_pos < len(pattern) and s_pos < len(s):
        # one arbitrary character
        if pattern[pat_pos] == '?':
            s_pos += 1
            pat_pos += 1
        # escape characters in pattern
        elif pattern[pat_pos] == '\\' and pat_pos + 1 < len(pattern):
            if pattern[pat_pos + 1] == s[s_pos]:
                s_pos += 1
                pat_pos += 2
            else:
                return s_pos
        # matching one character to pattern in brackets
        elif pattern[pat_pos] == '[':
            closing_bracket = pattern[pat_pos:].find(']')
            if closing_bracket == -1:
                raise StoragePatternError('no closing bracket found')
            if char_match_pattern(s[s_pos], pattern[pat_pos+1:pat_pos + closing_bracket]):
                s_pos += 1
                pat_pos += closing_bracket + 1
            else:
                return s_pos
        # any characters
        elif pattern[pat_pos] == '*':
            # skip all * if there are many
            while pat_pos < len(pattern) and pattern[pat_pos] == '*':
                pat_pos += 1
            # * was the last char in the pattern
            if pat_pos == len(pattern):
                return -1
            next_star = pattern[pat_pos:].find('*')
            # if there are no stars left in pattern
            # match remaining pattern to remaining string
            # from string's end
            if next_star == -1:
                remaining_pattern = pattern[pat_pos:]
                pat_len = pattern_length(remaining_pattern)
                diff = str_match_pattern_redis(s[-pat_len:], remaining_pattern)
                if diff == -1:
                    return -1
                else:
                    return len(s) - pat_len + diff
            # else match pattern between stars
            else:
                part_pattern = pattern[pat_pos: next_star]
                pat_len = pattern_length(part_pattern)
                new_s_start = s_pos
                diff = s_pos
                while new_s_start < len(s) - pat_len:
                    diff = str_match_pattern_redis(s[new_s_start:new_s_start+pat_len],part_pattern)
                    if diff == -1:
                        pat_pos = next_star
                        s_pos = new_s_start + pat_len
                        break
                    else:
                        new_s_start += 1
                else:
                    return s_pos
        elif pattern[pat_pos] == s[s_pos]:
            s_pos += 1
            pat_pos += 1
        else:
            return s_pos
    else:
        while pat_pos < len(pattern) and pattern[pat_pos] == '*':
            pat_pos += 1
        if pat_pos < len(pattern) or s_pos < len(s):
            return s_pos
        else:
            return -1


def char_match_pattern(char:str, pattern:str) -> bool:
    """
    Check if a character matches a pattern ([] pattern matching)
    :param char:
    :param pattern:
    :return: True if character matches pattern, False otherwise
    """
    if len(pattern):
        if pattern[0] == '^':
            inverse = True
            pattern = pattern[1:]
        else:
            inverse = False
        matches = set()
        pat_pos = 0
        while pat_pos < len(pattern):
            if pattern[pat_pos] == '-':
                if pat_pos - 1 >= 0 and pat_pos + 1 < len(pattern):
                    left = pattern[pat_pos-1]
                    right = pattern[pat_pos+1]
                    if left > right:
                        right, left = left, right
                    for i in range(ord(left), ord(right) + 1):
                        matches.add(chr(i))
                    pat_pos = pat_pos + 2
                else:
                    matches.add('-')
                    pat_pos += 1
            else:
                matches.add(pattern[pat_pos])
                pat_pos += 1
        if inverse:
            return char not in matches
        else:
            return char in matches
    else:
        return False


def pattern_length(pattern:str) -> int:
    """
    Count length of a string that the given
    pattern will match.
    :param pattern:
    :return: Length of a string that the pattern
    will match. -1 if pattern contains *
    """
    count = 0
    pat_pos = 0
    while pat_pos < len(pattern):
        if pattern[pat_pos] == '[':
            closing_bracket = pattern[pat_pos+1:].find(']')
            if closing_bracket == -1:
                raise StoragePatternError('no closing bracket found')
            pat_pos += closing_bracket + 2
            count += 1
        elif pattern[pat_pos] == '*':
            return -1
        else:
            count += 1
            pat_pos += 1
    return count
