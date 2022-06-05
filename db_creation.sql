CREATE TABLE denomination_partis (
    nom_annee_election VARCHAR(50),
    nom_derniere_election VARCHAR(50),
    PRIMARY KEY (nom_annee_election)
);

CREATE TABLE contexte_elections (
    annee INT,
    parti_vainqueur_presidentielles VARCHAR(50),
    PRIMARY KEY (annee)
);

CREATE TABLE tags (
    tag_id SERIAL,
    tag VARCHAR(255),
    PRIMARY KEY (tag_id)
);

CREATE TABLE candidats (
    id SERIAL,
    prenom VARCHAR(100) NOT NULL,
    nom VARCHAR(100) NOT NULL,
    sexe VARCHAR(1),
    date_naissance DATE,
    id_twitter BIGINT UNIQUE,
    username VARCHAR(255) UNIQUE,
    compte_verifie BOOLEAN,
    date_creation_compte DATE,
    PRIMARY KEY (id)
);

CREATE TABLE tweets(
    tweet_id BIGINT,
    author_id INT,
    publication_date DATE,
    like_counts INT,
    reply_counts INT,
    retweet_counts INT,
    quote_counts INT,
    reply_settings VARCHAR(30),
    possibly_sensitive BOOLEAN,
    label VARCHAR(10),
    polarity FLOAT,
    content VARCHAR(255),
    PRIMARY KEY (tweet_id),
    FOREIGN KEY (author_id) REFERENCES candidats(id_twitter)
);

CREATE TABLE affiliation_elections (
    id SERIAL,
    id_candidat INT,
    annee_election INT,
    nom_annee_election VARCHAR(50),
    code_departement INT,
    code_circonscription VARCHAR(10),
    sortant BOOLEAN,
    dissident BOOLEAN,
    resultat_election BOOLEAN,
    PRIMARY KEY (id),
    FOREIGN KEY (id_candidat) REFERENCES candidats(id),
    FOREIGN KEY (annee_election) REFERENCES contexte_elections(annee),
    FOREIGN KEY (nom_annee_election) REFERENCES denomination_partis(nom_annee_election)
);

CREATE TABLE tweets_tags (
    tweet_id INT,
    tag_id INT,
    PRIMARY KEY (tweet_id, tag_id),
    FOREIGN KEY (tweet_id) REFERENCES tweets(tweet_id),
    FOREIGN KEY (tag_id) REFERENCES tags(tag_id)
);