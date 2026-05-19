-- Source SHA-256: 54a5f2abccba7ea05e85b221b539c879c3aebed6b66691c42163b02f17546bb2

CREATE EXTENSION IF NOT EXISTS vector;

DROP INDEX IF EXISTS idx_documents_without_embedding;
DROP INDEX IF EXISTS documents_embedding_hnsw_idx;
DROP INDEX IF EXISTS documents_id_idx;
DROP TABLE IF EXISTS documents CASCADE;

CREATE TABLE IF NOT EXISTS shield_documents (
    id BIGSERIAL PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL CHECK (btrim(title) <> ''),
    body TEXT NOT NULL CHECK (btrim(body) <> ''),
    category TEXT NOT NULL DEFAULT 'general' CHECK (btrim(category) <> ''),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    priority INTEGER NOT NULL DEFAULT 0,
    embedding vector(512),
    title_search tsvector GENERATED ALWAYS AS (to_tsvector('russian', title)) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS shield_documents_enabled_priority_idx
    ON shield_documents (enabled, priority DESC, updated_at DESC);

CREATE INDEX IF NOT EXISTS shield_documents_title_search_idx
    ON shield_documents USING gin (title_search);

CREATE INDEX IF NOT EXISTS shield_documents_embedding_hnsw_idx
    ON shield_documents USING hnsw (embedding vector_cosine_ops)
    WHERE embedding IS NOT NULL;

CREATE OR REPLACE FUNCTION shield_documents_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS shield_documents_updated_at ON shield_documents;
CREATE TRIGGER shield_documents_updated_at
    BEFORE UPDATE ON shield_documents
    FOR EACH ROW
    EXECUTE FUNCTION shield_documents_set_updated_at();

INSERT INTO shield_documents (slug, title, body, category, enabled, priority)
VALUES
(
    'self_harm_crisis',
    'Self-harm suicide suicidal ideation wanting to die I want to die I do not want to live hurt myself cut myself overdose pills tablets jump hang kill myself самоубийство суицид хочу умереть не хочу жить покончить с собой причинить себе вред порезать себя таблетки выпить таблетки прыгнуть повеситься',
    $shield$When the current turn involves self-harm or suicide risk, prioritize immediate safety and connection. Respond calmly and directly; acknowledge pain without romanticizing it. Encourage the user to contact local emergency services or a trusted nearby person if there is immediate danger. If the user is in the United States, mention 988 as a crisis lifeline option. Ask one concrete safety-oriented question only when useful, such as whether they can move away from means or contact someone now. Do not provide methods, lethal details, encouragement, or anything that could increase capability for self-harm.$shield$,
    'self_harm',
    TRUE,
    100
),
(
    'psychosis_delusion_deescalation',
    'Psychosis delusion paranoia voices mania reality break mind control gang stalking surveillance secret signs спецслужбы следят за мной читают мысли голоса приказывают тайные знаки бред психоз паранойя преследуют',
    $shield$When the current turn suggests delusion, psychosis, paranoia, mania, or loss of reality testing, do not validate the belief as fact and do not argue aggressively. Offer grounding, uncertainty, and practical support: rest, hydration, reduce stimulation, contact a trusted person, clinician, crisis line, or emergency services if there is danger. Avoid escalating conspiratorial details, special missions, secret signs, or claims that the model has hidden knowledge. Keep the tone steady, kind, and reality-anchored.$shield$,
    'mental_health',
    TRUE,
    95
),
(
    'csam_child_safety',
    'CSAM child sexual abuse material sexual content involving minors underage nude teen minor grooming exploit lolicon loli школьница школьницу 12 13 14 летняя летнюю несовершеннолетняя несовершеннолетние сексуальный контент интимные фото дети секс',
    $shield$If the current turn asks for sexual content involving minors, child sexual abuse material, exploitation, grooming, or instructions to obtain or create such material, refuse the unsafe part immediately and briefly. Do not transform, eroticize, describe, roleplay, facilitate access, or provide procedural help. Redirect to child safety, reporting, and getting help if the user is at risk of offending. Safe discussion is limited to prevention, support for victims, reporting, and non-graphic legal or safety information.$shield$,
    'child_safety',
    TRUE,
    100
),
(
    'violent_harm_weapons',
    'Violence weapons bomb shooting murder stabbing assault making weapons explosive kill someone hurt someone attack самодельное оружие взрывчатка сделать бомбу зарезать убить напасть оружие нападение',
    $shield$When the current turn seeks to plan, enable, optimize, or carry out violence or weapon misuse, refuse the dangerous assistance. Do not provide instructions, targeting, acquisition, concealment, or tactical advice. If the user may imminently harm someone, encourage distance from weapons, contacting emergency services, or reaching a trusted person. Offer safe alternatives such as de-escalation, conflict support, legal/safety resources, or high-level non-operational information.$shield$,
    'violence',
    TRUE,
    90
),
(
    'coercion_abuse_stalking',
    'Coercion stalking blackmail doxxing harassment intimate partner abuse tracking someone spy on ex threaten manipulate шантаж сталкинг преследование доксинг следить за бывшей пробить адрес заставить угрожать',
    $shield$When the current turn asks to coerce, stalk, track, blackmail, dox, harass, manipulate, or abuse another person, refuse the unsafe assistance. Do not help with surveillance, pressure tactics, threats, bypassing boundaries, or exposing private data. Redirect toward consent, de-escalation, legal options, personal safety planning, and support resources for people experiencing abuse or obsessive fixation.$shield$,
    'abuse',
    TRUE,
    85
),
(
    'medical_substance_emergency',
    'Medical emergency overdose poisoning chest pain stroke seizure dangerous withdrawal pills tablets drugs alcohol overdose poison ambulance emergency передозировка передоз выпил таблетки отравление скорая плохо после наркотиков боль в груди инсульт судороги ломка',
    $shield$When the current turn suggests a medical or substance emergency, keep the response non-diagnostic and urgent. Encourage contacting local emergency services, poison control where available, or immediate in-person medical help. For overdose or poisoning, do not suggest waiting it out. Avoid precise dosing, risky home treatment, or reassurance that could delay care. Provide simple stabilizing steps only when broadly safe, such as staying with the person, following dispatcher instructions, and avoiding additional substances.$shield$,
    'medical',
    TRUE,
    90
)
ON CONFLICT (slug) DO UPDATE SET
    title = EXCLUDED.title,
    body = EXCLUDED.body,
    category = EXCLUDED.category,
    priority = EXCLUDED.priority,
    updated_at = CURRENT_TIMESTAMP;
