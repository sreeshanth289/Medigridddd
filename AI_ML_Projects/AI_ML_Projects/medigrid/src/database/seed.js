/**
 * MediGrid - Seed Data Generator
 * Seeds 42 real hospitals across Hyderabad/Secunderabad with 4,200+ beds.
 */

const { getDatabase } = require('./db');

const HYDERABAD_HOSPITALS = [
  { id: 'hosp_apollo_jh', name: 'Apollo Hospitals', code: 'APOLLO-JH', zone: 'West (Jubilee Hills)', lat: 17.4156, lon: 78.4124, address: 'Road No 72, Film Nagar, Jubilee Hills', phone: '+91-40-23607777', contact: 'Dr. K. Srinivas' },
  { id: 'hosp_kims_sec', name: 'KIMS Hospitals', code: 'KIMS-SEC', zone: 'North (Secunderabad)', lat: 17.4399, lon: 78.4983, address: '1-8-31/1, Minister Rd, Secunderabad', phone: '+91-40-44885000', contact: 'Dr. B. Bhaskar Rao' },
  { id: 'hosp_yashoda_som', name: 'Yashoda Hospitals Somajiguda', code: 'YASHODA-SOM', zone: 'Central (Somajiguda)', lat: 17.4241, lon: 78.4552, address: 'Raj Bhavan Rd, Matha Nagar, Somajiguda', phone: '+91-40-45674567', contact: 'Dr. G. Rao' },
  { id: 'hosp_yashoda_sec', name: 'Yashoda Hospitals Secunderabad', code: 'YASHODA-SEC', zone: 'North (Secunderabad)', lat: 17.4435, lon: 78.4989, address: 'Alexander Rd, Karkhana, Secunderabad', phone: '+91-40-27713333', contact: 'Dr. V. Prasad' },
  { id: 'hosp_yashoda_mal', name: 'Yashoda Hospitals Malakpet', code: 'YASHODA-MAL', zone: 'East (Malakpet)', lat: 17.3762, lon: 78.4947, address: 'Nalgonda X Roads, Malakpet', phone: '+91-40-24555555', contact: 'Dr. P. Surendra' },
  { id: 'hosp_nims_punj', name: "Nizam's Institute of Medical Sciences (NIMS)", code: 'NIMS-PUNJ', zone: 'Central (Punjagutta)', lat: 17.4239, lon: 78.4526, address: 'Punjagutta Rd, Hyderabad', phone: '+91-40-23489000', contact: 'Dr. N. Bheerappa' },
  { id: 'hosp_gandhi_mush', name: 'Gandhi Hospital', code: 'GANDHI-MUSH', zone: 'North-East (Musheerabad)', lat: 17.4245, lon: 78.5028, address: 'Padmarao Nagar, Musheerabad', phone: '+91-40-27505566', contact: 'Dr. M. Raja Rao' },
  { id: 'hosp_osmania_afz', name: 'Osmania General Hospital', code: 'OSMANIA-AFZ', zone: 'South (Afzal Gunj)', lat: 17.3753, lon: 78.4735, address: 'Afzal Gunj, High Court Rd, Hyderabad', phone: '+91-40-24600121', contact: 'Dr. B. Nagender' },
  { id: 'hosp_care_banj', name: 'Care Hospitals Banjara Hills', code: 'CARE-BANJ', zone: 'Central (Banjara Hills)', lat: 17.4172, lon: 78.4482, address: 'Road No 1, Banjara Hills', phone: '+91-40-61656565', contact: 'Dr. S. Reddy' },
  { id: 'hosp_care_hitec', name: 'Care Hospitals Hitec City', code: 'CARE-HITEC', zone: 'West (Hitec City)', lat: 17.4447, lon: 78.3752, address: 'Old Mumbai Hwy, Jayabheri Pine Valley', phone: '+91-40-67206565', contact: 'Dr. R. Verma' },
  { id: 'hosp_care_namp', name: 'Care Hospitals Nampally', code: 'CARE-NAMP', zone: 'Central (Nampally)', lat: 17.3850, lon: 78.4680, address: 'Mukarram Jahi Rd, Nampally', phone: '+91-40-30418888', contact: 'Dr. A. Khan' },
  { id: 'hosp_aig_gach', name: 'AIG Hospitals', code: 'AIG-GACH', zone: 'West (Gachibowli)', lat: 17.4438, lon: 78.3662, address: '1-66/AIG/1, Mindspace Rd, Gachibowli', phone: '+91-40-42444222', contact: 'Dr. D. Nageshwar Reddy' },
  { id: 'hosp_cont_gach', name: 'Continental Hospitals', code: 'CONT-GACH', zone: 'West (Financial District)', lat: 17.4187, lon: 78.3421, address: 'Plot No 3, Road No 2, IT & Financial Dist', phone: '+91-40-67000000', contact: 'Dr. Guru N. Reddy' },
  { id: 'hosp_sunshine_gach', name: 'Sunshine Hospitals Gachibowli', code: 'SUNSHINE-GACH', zone: 'West (Gachibowli)', lat: 17.4326, lon: 78.3582, address: 'Opp DLF Cyber City, Gachibowli', phone: '+91-40-44550000', contact: 'Dr. A. V. Gurava Reddy' },
  { id: 'hosp_rainbow_banj', name: "Rainbow Children's Hospital Banjara", code: 'RAINBOW-BANJ', zone: 'Central (Banjara Hills)', lat: 17.4190, lon: 78.4420, address: 'Road No 2, Banjara Hills', phone: '+91-40-44665555', contact: 'Dr. Ramesh Kancharla' },
  { id: 'hosp_rainbow_kond', name: "Rainbow Children's Hospital Kondapur", code: 'RAINBOW-KOND', zone: 'West (Kondapur)', lat: 17.4644, lon: 78.3615, address: 'Kondapur Main Rd, Hyderabad', phone: '+91-40-42665555', contact: 'Dr. D. Swetha' },
  { id: 'hosp_star_banj', name: 'Star Hospitals Banjara Hills', code: 'STAR-BANJ', zone: 'Central (Banjara Hills)', lat: 17.4181, lon: 78.4357, address: 'Road No 10, Banjara Hills', phone: '+91-40-44777777', contact: 'Dr. Gopichand Mannam' },
  { id: 'hosp_star_nanak', name: 'Star Hospitals Nanakramguda', code: 'STAR-NANAK', zone: 'West (Nanakramguda)', lat: 17.4150, lon: 78.3520, address: 'Financial District, Nanakramguda', phone: '+91-40-44778888', contact: 'Dr. K. Ramesh' },
  { id: 'hosp_medicover_hitec', name: 'Medicover Hospitals Hitec City', code: 'MEDICOVER-HITEC', zone: 'West (Hitec City)', lat: 17.4485, lon: 78.3812, address: 'Behind Cyber Towers, Hitec City', phone: '+91-40-68334455', contact: 'Dr. A. Krishna' },
  { id: 'hosp_medicover_sec', name: 'Medicover Hospitals Secretariat', code: 'MEDICOVER-SEC', zone: 'Central (Secretariat)', lat: 17.4082, lon: 78.4715, address: 'Opp Secretariat, Saifabad', phone: '+91-40-68334466', contact: 'Dr. S. Mohanty' },
  { id: 'hosp_apollo_ameer', name: 'Apollo Spectra Hospitals Ameerpet', code: 'APOLLO-AMEER', zone: 'Central (Ameerpet)', lat: 17.4375, lon: 78.4482, address: 'Near Mythrivanam, Ameerpet', phone: '+91-40-44442222', contact: 'Dr. H. Sharma' },
  { id: 'hosp_apollo_cradle', name: 'Apollo Cradle Jubilee Hills', code: 'APOLLO-CRADLE', zone: 'West (Jubilee Hills)', lat: 17.4285, lon: 78.4110, address: 'Road No 36, Jubilee Hills', phone: '+91-40-44244424', contact: 'Dr. P. Vani' },
  { id: 'hosp_fernandez_hyder', name: 'Fernandez Hospital Hyderguda', code: 'FERNANDEZ-HYD', zone: 'Central (Hyderguda)', lat: 17.3970, lon: 78.4835, address: '3-6-282, Hyderguda, Hyderabad', phone: '+91-40-40222300', contact: 'Dr. Evita Fernandez' },
  { id: 'hosp_fernandez_bogul', name: 'Fernandez Hospital Bogulkunta', code: 'FERNANDEZ-BOG', zone: 'Central (Bogulkunta)', lat: 17.3912, lon: 78.4870, address: 'Abids, Bogulkunta', phone: '+91-40-40222400', contact: 'Dr. Tarakeshwari' },
  { id: 'hosp_pace_hitec', name: 'PACE Hospitals Hitec City', code: 'PACE-HITEC', zone: 'West (Hitec City)', lat: 17.4498, lon: 78.3770, address: 'Near Image Hospitals Lane, Madhapur', phone: '+91-40-48486868', contact: 'Dr. Govind Verma' },
  { id: 'hosp_tx_uppal', name: 'TX Hospitals Uppal', code: 'TX-UPPAL', zone: 'East (Uppal)', lat: 17.4018, lon: 78.5602, address: 'Survey No 84, Uppal Main Road', phone: '+91-40-45667788', contact: 'Dr. R. Chowdary' },
  { id: 'hosp_tx_banj', name: 'TX Hospitals Banjara Hills', code: 'TX-BANJ', zone: 'Central (Banjara Hills)', lat: 17.4215, lon: 78.4380, address: 'Road No 12, Banjara Hills', phone: '+91-40-45668899', contact: 'Dr. M. Sastry' },
  { id: 'hosp_tx_kachi', name: 'TX Hospitals Kachiguda', code: 'TX-KACHI', zone: 'Central (Kachiguda)', lat: 17.3920, lon: 78.4975, address: 'Station Rd, Kachiguda', phone: '+91-40-45669900', contact: 'Dr. K. Srinath' },
  { id: 'hosp_gleneagles_lak', name: 'Gleneagles Global Hospitals', code: 'GLENEAGLES-LAK', zone: 'Central (Lakdikapul)', lat: 17.4055, lon: 78.4632, address: '6-1-1070/1 to 4, Lakdikapul', phone: '+91-40-23491000', contact: 'Dr. K. Ravindranath' },
  { id: 'hosp_aster_ameer', name: 'Aster Prime Hospital', code: 'ASTER-PRIME', zone: 'Central (Ameerpet)', lat: 17.4402, lon: 78.4445, address: 'Plot No 4, HMDA Maitrivanam, Ameerpet', phone: '+91-40-49594959', contact: 'Dr. Satish Pawar' },
  { id: 'hosp_olive_mehd', name: 'Olive Hospital', code: 'OLIVE-MEHD', zone: 'South-West (Mehdipatnam)', lat: 17.3915, lon: 78.4350, address: '12-2-718/3, Nanal Nagar, Mehdipatnam', phone: '+91-40-23512222', contact: 'Dr. Syed Mustaq' },
  { id: 'hosp_premier_mehd', name: 'Premier Hospital', code: 'PREMIER-MEHD', zone: 'South-West (Mehdipatnam)', lat: 17.3942, lon: 78.4290, address: 'Humayun Nagar, Mehdipatnam', phone: '+91-40-23533444', contact: 'Dr. M. A. Baksh' },
  { id: 'hosp_virinchi_banj', name: 'Virinchi Hospitals', code: 'VIRINCHI-BANJ', zone: 'Central (Banjara Hills)', lat: 17.4120, lon: 78.4510, address: 'Road No 1, Banjara Hills', phone: '+91-40-46999999', contact: 'Dr. G. Rao' },
  { id: 'hosp_century_banj', name: 'Century Hospital', code: 'CENTURY-BANJ', zone: 'Central (Banjara Hills)', lat: 17.4165, lon: 78.4415, address: 'Road No 12, Banjara Hills', phone: '+91-40-67252525', contact: 'Dr. H. Mohan' },
  { id: 'hosp_omni_koth', name: 'Omni Hospitals Kothapet', code: 'OMNI-KOTH', zone: 'East (Kothapet)', lat: 17.3712, lon: 78.5390, address: 'NH 65, Dilsukhnagar, Kothapet', phone: '+91-40-41113333', contact: 'Dr. Alok Ranjan' },
  { id: 'hosp_omni_kukat', name: 'Omni Hospitals Kukatpally', code: 'OMNI-KUKAT', zone: 'North-West (Kukatpally)', lat: 17.4875, lon: 78.4120, address: 'Opp Pillar 818, Kukatpally', phone: '+91-40-41114444', contact: 'Dr. V. Naidu' },
  { id: 'hosp_citizens_nalla', name: 'Citizens Specialty Hospital', code: 'CITIZENS-NALLA', zone: 'West (Nallagandla)', lat: 17.4670, lon: 78.3090, address: 'Near Aparna Sarovar, Nallagandla', phone: '+91-40-67199999', contact: 'Dr. M. Sudhakar' },
  { id: 'hosp_archana_madi', name: 'Archana Hospital', code: 'ARCHANA-MADI', zone: 'North-West (Madinaguda)', lat: 17.4980, lon: 78.3370, address: 'Chanda Nagar, Madinaguda', phone: '+91-40-23045678', contact: 'Dr. C. Archana' },
  { id: 'hosp_bristle_hayath', name: 'Bristlecone Hospital', code: 'BRISTLE-HAYATH', zone: 'East (Hayathnagar)', lat: 17.3290, lon: 78.6010, address: 'Vijayawada Hwy, Hayathnagar', phone: '+91-40-24203333', contact: 'Dr. K. Srinivasulu' },
  { id: 'hosp_ankura_atta', name: 'Ankura Hospital Attapur', code: 'ANKURA-ATTA', zone: 'South (Attapur)', lat: 17.3730, lon: 78.4310, address: 'Pillar No 143, PVNR Expressway, Attapur', phone: '+91-40-49495959', contact: 'Dr. K. Vamsi' },
  { id: 'hosp_medicover_chanda', name: 'Medicover Hospitals Chandanagar', code: 'MEDICOVER-CHAND', zone: 'North-West (Chandanagar)', lat: 17.4950, lon: 78.3240, address: 'Opp GSM Mall, Chandanagar', phone: '+91-40-68334477', contact: 'Dr. B. Raman' },
  { id: 'hosp_esic_sanath', name: 'ESIC Medical College & Hospital', code: 'ESIC-SANATH', zone: 'North (Sanathnagar)', lat: 17.4480, lon: 78.4410, address: 'Sanath Nagar, Hyderabad', phone: '+91-40-23801122', contact: 'Dr. M. Srinivas' }
];

const BED_TYPES = [
  { type: 'ICU', count: 25, ward: 'Critical Care ICU' },
  { type: 'VENTILATOR', count: 15, ward: 'Ventilator / CCU' },
  { type: 'GENERAL', count: 40, ward: 'General Medical Ward' },
  { type: 'OXYGEN', count: 15, ward: 'High Dependency HDU' },
  { type: 'ISOLATION', count: 5, ward: 'Isolation & Infection Control' }
];

function seedDatabase() {
  const dbService = getDatabase();
  const db = dbService.db;

  const existing = db.prepare(`SELECT COUNT(*) as cnt FROM hospitals`).get();
  if (existing.cnt >= 40) {
    console.log(`[Seed] Database already seeded with ${existing.cnt} hospitals.`);
    return;
  }

  console.log(`[Seed] Seeding ${HYDERABAD_HOSPITALS.length} hospitals and 4,200+ beds...`);

  const insertHospital = db.prepare(`
    INSERT OR REPLACE INTO hospitals (id, name, code, zone, lat, lon, address, phone, contact_person)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertBed = db.prepare(`
    INSERT OR REPLACE INTO beds (id, hospital_id, ward_name, bed_number, bed_type, status, patient_id, patient_name, reserved_until, reservation_id, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const now = Date.now();
  let totalBedsCreated = 0;

  // Run within SQLite transaction for high speed
  db.exec('BEGIN TRANSACTION;');

  try {
    for (const h of HYDERABAD_HOSPITALS) {
      insertHospital.run(h.id, h.name, h.code, h.zone, h.lat, h.lon, h.address, h.phone, h.contact);

      let bedIndex = 1;
      for (const bt of BED_TYPES) {
        for (let i = 1; i <= bt.count; i++) {
          const bedId = `bed_${h.code.toLowerCase()}_${bt.type.toLowerCase()}_${String(i).padStart(3, '0')}`;
          const bedNumber = `${bt.type[0]}-${String(i).padStart(2, '0')}`;

          // Realistic distribution:
          // ~65% occupied, ~25% available, ~7% cleaning, ~3% reserved
          const rand = Math.random();
          let status = 'AVAILABLE';
          let patientId = null;
          let patientName = null;
          let reservedUntil = null;
          let reservationId = null;

          if (rand < 0.65) {
            status = 'OCCUPIED';
            patientId = 'PAT-' + Math.floor(10000 + Math.random() * 90000);
            patientName = `Patient ${patientId.substring(4)}`;
          } else if (rand < 0.72) {
            status = 'CLEANING';
          } else if (rand < 0.75) {
            status = 'RESERVED';
            reservationId = 'RES-' + Math.floor(1000 + Math.random() * 9000);
            reservedUntil = now + Math.floor(5 * 60 * 1000 + Math.random() * 10 * 60 * 1000); // 5-15 min TTL
          } else {
            status = 'AVAILABLE';
          }

          insertBed.run(
            bedId,
            h.id,
            bt.ward,
            bedNumber,
            bt.type,
            status,
            patientId,
            patientName,
            reservedUntil,
            reservationId,
            now
          );
          totalBedsCreated++;
          bedIndex++;
        }
      }
    }

    db.exec('COMMIT;');
    console.log(`[Seed] Successfully seeded ${HYDERABAD_HOSPITALS.length} hospitals and ${totalBedsCreated} beds!`);
  } catch (err) {
    db.exec('ROLLBACK;');
    console.error('[Seed] Error seeding database:', err);
    throw err;
  }
}

if (require.main === module) {
  seedDatabase();
}

module.exports = { seedDatabase, HYDERABAD_HOSPITALS };
