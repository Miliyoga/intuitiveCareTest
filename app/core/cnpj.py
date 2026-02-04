import re

def normalize_cnpj(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"\D", "", str(value))

def is_valid_cnpj(value: str | None) -> bool:
    cnpj = normalize_cnpj(value)
    if len(cnpj) != 14:
        return False
    if cnpj == cnpj[0] * 14:
        return False

    def calc_digit(base: str, weights: list[int]) -> str:
        s = sum(int(d) * w for d, w in zip(base, weights))
        r = s % 11
        return "0" if r < 2 else str(11 - r)

    w1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    w2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    d1 = calc_digit(cnpj[:12], w1)
    d2 = calc_digit(cnpj[:12] + d1, w2)
    return cnpj[-2:] == d1 + d2
