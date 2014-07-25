#include<iostream>
#include<fstream>
#include<cstdlib>
#include<string>
#include"tinyxml.h"
#include"tinystr.h"
using namespace std;

/*
Parameters:
# MapJavaHeap:  mapred.map.child.java.opt
# RedJavaHeap:  mapred.reduce.java.opt
# SortMB:       io.sort.mb
# SortPer:      io.sort.percent
# JvmReuse:     mapred.job.reuse.jvm.num.task
# ShufflePer:   mapred.job.shuffle.input.buffer.percent
./revise_conf $EXP/conf_new/mapred-site.xml $MapJavaHeap $RedJavaHeap $SortMB $SortPer $JvmReuse 
*/


class config{
public:
	config(string config_path)
	{	
		this->config_path = config_path;
		this->Conf_Xml.LoadFile(config_path.c_str());		//config_path="/home/trend-hadoop/expr/conf/conf_new/mapred-site.xml"
		this->conf = Conf_Xml.RootElement();	
	}

	void revise_config(string config_parameter,string config_value)
	{
		TiXmlElement *property = new TiXmlElement("property");
	        TiXmlElement *name = new TiXmlElement("name");
	        TiXmlElement *value = new TiXmlElement("value");
	        TiXmlText *name_text = new TiXmlText(config_parameter.c_str());
	        TiXmlText *value_text = new TiXmlText(config_value.c_str());

	        name->LinkEndChild(name_text);
	        value->LinkEndChild(value_text);
	        property->LinkEndChild(name);
	        property->LinkEndChild(value);
	        (this->conf)->LinkEndChild(property);

	}
	
	void save_config()
	{
		(this->Conf_Xml).SaveFile();
	}


	

	
	void revise_xml_stylesheet()
	{
		string search_string = "<?xml ?>";
	        string replace_string = "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?> ";
	        string inbuf;
		string tmp_config_path = (this->config_path)+".tmp";
	        fstream input_file(this->config_path.c_str(), ios::in);
	        ofstream output_file(tmp_config_path.c_str());
		while (getline(input_file,inbuf))
	        {
	
	                int spot = inbuf.find(search_string);
	                if(spot >= 0)
	                {
	                        output_file<<replace_string<<endl;
	                }
	                else
	                {
	                        output_file << inbuf << endl;
	                }

	        }
		
		
		string sys_cmd = "mv "+tmp_config_path+" "+(this->config_path);
	        system(sys_cmd.c_str());

	}




	void print_config()
	{
		TiXmlElement *conf = Conf_Xml.RootElement();
		cout<<conf->Value()<<endl;
	}


private:
	TiXmlDocument Conf_Xml;
	TiXmlElement *conf;
	string config_path;
};

int main(int argc, char* argv[])
{

	int num_arg=1;
	
	if(argc%2==0)
	{cout<<"wrong argument";}
	else
	{
	
		config new_config("/home/trend-hadoop/expr/conf/conf_new/mapred-site.xml");
	
		while(num_arg!=argc)
		{
			cout<<argv[num_arg]<<'\t'<<argv[num_arg+1]<<endl;
			new_config.revise_config(argv[num_arg],argv[num_arg+1]);
			num_arg+=2;
		}

		new_config.save_config();
		new_config.print_config();
		new_config.revise_xml_stylesheet();
	}
	
	
	return 0;
}
