package resources.entities;

import org.terifan.raccoon.annotations.Id;


public class _Person1K
{
	@Id public String _firstName;
	public String lastName;
	public String address;
	public String postCode;
	public String city;
	public String country;
	public String phone;
	public String mobilePhone;
	public String employeer;
	public double height;
	public double weight;


	public _Person1K()
	{
	}


	public _Person1K(String aFirstName, String aLastName, String aAddress, String aPostCode, String aCity, String aCountry, String aPhone, String aObilePhone, String aEmployeer, double aHeight, double aWeight)
	{
		this._firstName = aFirstName;
		this.lastName = aLastName;
		this.address = aAddress;
		this.postCode = aPostCode;
		this.city = aCity;
		this.country = aCountry;
		this.phone = aPhone;
		this.mobilePhone = aObilePhone;
		this.employeer = aEmployeer;
		this.height = aHeight;
		this.weight = aWeight;
	}


	@Override
	public String toString()
	{
		return "_Person1K{" + "_firstName=" + _firstName + ", lastName=" + lastName + ", address=" + address + ", postCode=" + postCode + ", city=" + city + ", country=" + country + ", phone=" + phone + ", mobilePhone=" + mobilePhone + ", employeer=" + employeer + ", height=" + height + ", weight=" + weight + '}';
	}
}