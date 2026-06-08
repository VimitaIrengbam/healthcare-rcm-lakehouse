-- =============================================================================
-- Unity Catalog column-mask functions (Phase 4 / governance, query-time enforcement)
-- Members of the `rcm_phi_readers` group see raw values; everyone else sees masked.
-- Run once after silver tables exist. Complements the write-time redaction in masking.py.
-- =============================================================================

-- 1) Mask function for free-text PII (names, etc.)
CREATE OR REPLACE FUNCTION rcm.silver.mask_text(val STRING)
RETURN CASE WHEN is_account_group_member('rcm_phi_readers') THEN val ELSE '***' END;

-- 2) Mask function for SSN-like values (keep last 4 for authorized, fully hide otherwise)
CREATE OR REPLACE FUNCTION rcm.silver.mask_ssn(val STRING)
RETURN CASE
         WHEN is_account_group_member('rcm_phi_readers') THEN val
         ELSE 'XXX-XX-XXXX'
       END;

-- 3) Apply masks to the patient dimension columns
ALTER TABLE rcm.silver.patient ALTER COLUMN firstname SET MASK rcm.silver.mask_text;
ALTER TABLE rcm.silver.patient ALTER COLUMN ssn       SET MASK rcm.silver.mask_ssn;

-- 4) (Optional) row filter example — restrict a group to a single hospital
-- CREATE OR REPLACE FUNCTION rcm.silver.hospital_filter(hid STRING)
-- RETURN is_account_group_member('rcm_admins') OR hid = 'hospital_a';
-- ALTER TABLE rcm.silver.patient SET ROW FILTER rcm.silver.hospital_filter ON (hospital_id);

-- Verify (run as a non-member to see masked output):
-- SELECT patient_id, firstname, ssn FROM rcm.silver.patient LIMIT 10;
