using System.Collections.Generic;

namespace azure_cosmosdb_geospatial
{
    public class Spatial
    {
        public string name { get; set; }

        public string id { get; set; }

        public string nametype { get; set; }

        public string recclass { get; set; }

        public string mass { get; set; }

        public string fall { get; set; }

        public string year { get; set; }

        public string reclat { get; set; }

        public string reclong { get; set; }

        public Geolocation geolocation { get; set; }
    }

    public class Geolocation
    {
        public string type { get; set; }
        public List<float> coordinates { get; set; }
    }
}
