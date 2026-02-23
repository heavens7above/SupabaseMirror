const { createClient } = require("@supabase/supabase-js");
require("dotenv").config();

const supabaseUrl = (process.env.SUPABASE_URL || "").replace(/^"|"$/g, '');
const supabaseServiceRoleKey = (process.env.SUPABASE_SERVICE_ROLE_KEY || "").replace(/^"|"$/g, '');

if (!supabaseUrl || !supabaseServiceRoleKey) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY. Please add these in your Railway project Variables tab.");
}

const supabase = createClient(supabaseUrl, supabaseServiceRoleKey);

module.exports = supabase;
