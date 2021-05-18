package by.sanko.spark.two.entity;

public class HotelData {
    private long id;
    private String name;
    private String country;
    private String city;
    private String address;
    private double longitude;
    private double latitude;
    private String geoHash;

    public HotelData(long id, String name, String country, String city, String address, double longitude, double latitude, String geoHash) {
        this.id = id;
        this.name = name;
        this.country = country;
        this.city = city;
        this.address = address;
        this.longitude = longitude;
        this.latitude = latitude;
        this.geoHash = geoHash;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getCountry() {
        return country;
    }

    public String getCity() {
        return city;
    }

    public String getAddress() {
        return address;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public String getGeoHash() {
        return geoHash;
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = result * 19 + geoHash.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "HotelData{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", country='" + country + '\'' +
                ", city='" + city + '\'' +
                ", address='" + address + '\'' +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", geoHash='" + geoHash + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this){
            return true;
        }
        if (obj == null || getClass() != obj.getClass()){
            return false;
        }
        HotelData data = (HotelData) obj;
        return data.id == id && data.getName().equals(name) && data.getCountry().equals(country) &&
                city.equals(data.getCity()) && address.equals(data.getAddress()) && longitude == data.getLongitude() &&
                latitude == data.getLatitude() && geoHash.equals(data.getGeoHash());
    }
}