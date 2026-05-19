-- Source SHA-256: 2a2b6cc2f8b34c37df61fc653403e3dd41cd66b8345956ab09ae5c7bd2121f53

DROP INDEX IF EXISTS shield_documents_title_search_idx;
ALTER TABLE shield_documents DROP COLUMN IF EXISTS title_search;
ALTER TABLE shield_documents
    ADD COLUMN title_search tsvector GENERATED ALWAYS AS (to_tsvector('simple', title)) STORED;
CREATE INDEX IF NOT EXISTS shield_documents_title_search_idx
    ON shield_documents USING gin (title_search);

UPDATE shield_documents AS d
SET title = v.title,
    embedding = NULL,
    updated_at = CURRENT_TIMESTAMP
FROM (
    VALUES
    ('self_harm_crisis', 'Self-harm, suicide, suicidal ideation, wanting to die, self-injury, overdose, прыгнуть, покончить с собой, суицид, самоповреждение'),
    ('psychosis_delusion_deescalation', 'Psychosis, delusion, paranoia, voices, mania, reality break, mind control, gang stalking, бред, психоз, паранойя, голоса, преследуют'),
    ('csam_child_safety', 'CSAM, sexual content involving minors, child sexual abuse material, underage nude sexual, несовершеннолетние сексуальный контент, дети секс'),
    ('violent_harm_weapons', 'Violence, weapons, bomb, shooting, murder, stabbing, assault, making weapons, убить, оружие, бомба, нападение'),
    ('coercion_abuse_stalking', 'Coercion, stalking, blackmail, doxxing, harassment, intimate partner abuse, tracking someone, шантаж, сталкинг, преследование, доксинг'),
    ('medical_substance_emergency', 'Medical emergency, overdose, poisoning, chest pain, stroke, seizure, dangerous withdrawal, передозировка, отравление, скорая, ломка')
) AS v(slug, title)
WHERE d.slug = v.slug
  AND d.title IS DISTINCT FROM v.title;
