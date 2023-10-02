from typing import List, Dict
import json
import requests

from flytekit import task, workflow, ImageSpec
from random import randint

SPACY_URL = "http://host.docker.internal:8080/ent"

@task(container_image="localhost:30000/retrieve-books:latest")
def retrieve_books() -> List[str]:
    import gutenbergpy.textget

    random_book_ids = []
    for i in range(1):  # tweak for more books at once
        # this might include duplicates, not a worry for now
        random_book_ids.append(randint(1, 10000))

    texts = []
    for book_id in random_book_ids:
        raw_book = gutenbergpy.textget.get_text_by_id(book_id)
        clean_book = gutenbergpy.textget.strip_headers(raw_book)
        texts.append(clean_book.decode('UTF-8'))

    return texts

@task
def to_sentences(texts: List[str]) -> List[List[str]]:
    texts_sentences = []
    for text in texts:
        sentences = []
        for candidate in text.split("\n"):
            if len(candidate) > 50:  # only consider sentences with more than 50 characters
                sentences.append(candidate)
            if len(sentences) == 10:  # tweak for more sentences at once
                break

        texts_sentences.append(sentences)

    return texts_sentences

@task
def entity_extraction(texts_sentences: List[List[str]]) -> List[Dict]:
    sentences_with_entities = []
    for sentences in texts_sentences:
        for sentence in sentences:
            headers = {'content-type': 'application/json'}
            d = {'text': sentence, 'model': 'en'}

            response = requests.post(SPACY_URL, data=json.dumps(d), headers=headers)
            r = response.json()

            sentences_with_entities.append({'sentence': sentence, 'entities': r})

    return sentences_with_entities

@task
def print_sentences(sentences_with_entities: List[Dict]) -> None:
    for sentence in sentences_with_entities:
        print(sentence)

@workflow
def nlp_wf() -> None:
    texts = retrieve_books()
    sentences = to_sentences(texts=texts)
    sentences_with_entities = entity_extraction(texts_sentences=sentences)
    print_sentences(sentences_with_entities=sentences_with_entities)


if __name__ == "__main__":
    print(f"Running nlp_wf() {nlp_wf()}")
