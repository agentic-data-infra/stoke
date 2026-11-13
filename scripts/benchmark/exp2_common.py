"""What every loader of Experiment 2 does to the tokenized documents: keep
those of int_score >= 3, language_score >= 0.9 and at least MIN_TOKENS
tokens, join them (each ends in the end-of-text token) and cut the stream
into sequences of SEQ tokens (inputs and shifted targets of SEQ - 1),
BATCH sequences per batch."""

BATCH = 32
SEQ = 4097
MIN_TOKENS = 64
END_OF_TEXT = 50256
FILTER_SQL = f"int_score >= 3 AND language_score >= 0.9 AND token_count >= {MIN_TOKENS}"


def keep(doc):
    return doc["int_score"] >= 3 and doc["language_score"] >= 0.9 and doc["token_count"] >= MIN_TOKENS
