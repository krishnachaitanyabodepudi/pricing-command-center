USE PricingDWH;
GO

MERGE pricing.dim_product AS target
USING (
    VALUES
    ('SKU-10001', 'Sterile Gloves Premium', 'Medical', 'MedTech', 1),
    ('SKU-10002', 'Surgical Scalpel Set', 'Surgical', 'SurgiPro', 1),
    ('SKU-10003', 'Antibiotic Ointment', 'Pharma', 'PharmCore', 1),
    ('SKU-10004', 'Digital Thermometer', 'Consumer', 'HealthHome', 1),
    ('SKU-10005', 'X-Ray Machine Mobile', 'Equipment', 'MedEquip', 1),
    ('SKU-10006', 'Disposable Syringes 10ml', 'Medical', 'MedTech', 1),
    ('SKU-10007', 'Surgical Mask N95', 'Medical', 'MedTech', 1),
    ('SKU-10008', 'Suture Kit Professional', 'Surgical', 'SurgiPro', 1),
    ('SKU-10009', 'Pain Relief Capsules', 'Pharma', 'PharmCore', 1),
    ('SKU-10010', 'Blood Pressure Monitor', 'Consumer', 'HealthHome', 1),
    ('SKU-10011', 'Ultrasound Scanner', 'Equipment', 'MedEquip', 1),
    ('SKU-10012', 'Bandage Roll 4in', 'Medical', 'MedTech', 1),
    ('SKU-10013', 'Surgical Forceps', 'Surgical', 'SurgiPro', 1),
    ('SKU-10014', 'Antihistamine Tablets', 'Pharma', 'PharmCore', 1),
    ('SKU-10015', 'First Aid Kit Deluxe', 'Consumer', 'HealthHome', 1),
    ('SKU-10016', 'ECG Machine Portable', 'Equipment', 'MedEquip', 1),
    ('SKU-10017', 'IV Catheter 18G', 'Medical', 'MedTech', 1),
    ('SKU-10018', 'Surgical Scissors', 'Surgical', 'SurgiPro', 1),
    ('SKU-10019', 'Cough Syrup', 'Pharma', 'PharmCore', 1),
    ('SKU-10020', 'Pulse Oximeter', 'Consumer', 'HealthHome', 1),
    ('SKU-10021', 'Defibrillator AED', 'Equipment', 'MedEquip', 1),
    ('SKU-10022', 'Gauze Pads 4x4', 'Medical', 'MedTech', 1),
    ('SKU-10023', 'Surgical Retractor', 'Surgical', 'SurgiPro', 1),
    ('SKU-10024', 'Antacid Tablets', 'Pharma', 'PharmCore', 1),
    ('SKU-10025', 'Neck Brace Adjustable', 'Consumer', 'HealthHome', 1),
    ('SKU-10026', 'Ventilator ICU', 'Equipment', 'MedEquip', 1),
    ('SKU-10027', 'Alcohol Swabs 100ct', 'Medical', 'MedTech', 1),
    ('SKU-10028', 'Surgical Clamp', 'Surgical', 'SurgiPro', 1),
    ('SKU-10029', 'Cold & Flu Medicine', 'Pharma', 'PharmCore', 1),
    ('SKU-10030', 'Knee Brace Support', 'Consumer', 'HealthHome', 1),
    ('SKU-10031', 'MRI Machine 3T', 'Equipment', 'MedEquip', 1),
    ('SKU-10032', 'Wound Dressing Pack', 'Medical', 'MedTech', 1),
    ('SKU-10033', 'Surgical Needle Holder', 'Surgical', 'SurgiPro', 1),
    ('SKU-10034', 'Vitamin D Supplements', 'Pharma', 'PharmCore', 1),
    ('SKU-10035', 'Ankle Support Wrap', 'Consumer', 'HealthHome', 1),
    ('SKU-10036', 'CT Scanner 64-Slice', 'Equipment', 'MedEquip', 1),
    ('SKU-10037', 'Adhesive Tape Medical', 'Medical', 'MedTech', 1),
    ('SKU-10038', 'Surgical Blade #11', 'Surgical', 'SurgiPro', 1),
    ('SKU-10039', 'Multivitamin Capsules', 'Pharma', 'PharmCore', 1),
    ('SKU-10040', 'Wrist Support Brace', 'Consumer', 'HealthHome', 1),
    ('SKU-10041', 'Infusion Pump', 'Equipment', 'MedEquip', 1),
    ('SKU-10042', 'Cotton Swabs Sterile', 'Medical', 'MedTech', 1),
    ('SKU-10043', 'Surgical Hemostat', 'Surgical', 'SurgiPro', 1),
    ('SKU-10044', 'Calcium Supplements', 'Pharma', 'PharmCore', 1),
    ('SKU-10045', 'Back Support Belt', 'Consumer', 'HealthHome', 1),
    ('SKU-10046', 'Patient Monitor', 'Equipment', 'MedEquip', 1),
    ('SKU-10047', 'Disposable Gloves Latex', 'Medical', 'MedTech', 1),
    ('SKU-10048', 'Surgical Probe', 'Surgical', 'SurgiPro', 1),
    ('SKU-10049', 'Iron Supplements', 'Pharma', 'PharmCore', 1),
    ('SKU-10050', 'Compression Stockings', 'Consumer', 'HealthHome', 1),
    ('SKU-10051', 'Autoclave Sterilizer', 'Equipment', 'MedEquip', 1),
    ('SKU-10052', 'Antiseptic Solution', 'Medical', 'MedTech', 1),
    ('SKU-10053', 'Surgical Sponge', 'Surgical', 'SurgiPro', 1),
    ('SKU-10054', 'Probiotic Capsules', 'Pharma', 'PharmCore', 1),
    ('SKU-10055', 'Elbow Support Brace', 'Consumer', 'HealthHome', 1)
) AS source (sku, product_name, category, brand, is_active)
ON target.sku = source.sku
WHEN MATCHED THEN
    UPDATE SET 
        product_name = source.product_name,
        category = source.category,
        brand = source.brand,
        is_active = source.is_active
WHEN NOT MATCHED THEN
    INSERT (sku, product_name, category, brand, is_active)
    VALUES (source.sku, source.product_name, source.category, source.brand, source.is_active);
GO


