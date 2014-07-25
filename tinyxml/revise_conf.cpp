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

int main(int argc, char* argv[])
{
	
	string MapJavaHeap = argv[1];
	string RedJavaHeap = argv[2];
	string SortMB = argv[3];
	string SortPer = argv[4];
	string JvmReuse = argv[5];
//	string ShufflePer = argv[6];
	

	TiXmlDocument Conf_Xml;
	Conf_Xml.LoadFile("/home/trend-hadoop/expr/conf/conf_new/mapred-site.xml");
	TiXmlElement *conf = Conf_Xml.RootElement();
//	cout<<conf->Value()<<endl;

//	TiXmlStylesheetReference *style_sheet = new TiXmlStylesheetReference("text/xsl","configuration.xsl");
//	TiXmlElement *declare = Conf_Xml.FirstChildElement();
//	string tmp = "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>";
//	TiXmlText *style_sheet = new TiXmlText(tmp.c_str());
//	Conf_Xml.InsertBeforeChild(declare,*style_sheet);

//=========================MapJavaHeap=======================================================

	TiXmlElement *property = new TiXmlElement("property");
	TiXmlElement *name = new TiXmlElement("name");
	TiXmlElement *value = new TiXmlElement("value");
	TiXmlText *name_text = new TiXmlText("mapred.map.child.java.opts");
	TiXmlText *value_text = new TiXmlText(MapJavaHeap.c_str());
	
	name->LinkEndChild(name_text);
	value->LinkEndChild(value_text);
	property->LinkEndChild(name);
	property->LinkEndChild(value);
	conf->LinkEndChild(property);

//======================ReduceJavaHeap========================================================


	property = new TiXmlElement("property");
	name = new TiXmlElement("name");
        value = new TiXmlElement("value");
        name_text = new TiXmlText("mapred.reduce.child.java.opts");
        value_text = new TiXmlText(RedJavaHeap.c_str());

        name->LinkEndChild(name_text);
        value->LinkEndChild(value_text);
        property->LinkEndChild(name);
        property->LinkEndChild(value);
        conf->LinkEndChild(property);
	
		
//==========================SortMB==========================================
	
	
        property = new TiXmlElement("property");
        name = new TiXmlElement("name");
        value = new TiXmlElement("value");
        name_text = new TiXmlText("io.sort.mb");
        value_text = new TiXmlText(SortMB.c_str());

        name->LinkEndChild(name_text);
        value->LinkEndChild(value_text);
        property->LinkEndChild(name);
        property->LinkEndChild(value);
        conf->LinkEndChild(property);

//======================================SortPer=====================================
	
	property = new TiXmlElement("property");
        name = new TiXmlElement("name");
        value = new TiXmlElement("value");
        name_text = new TiXmlText("io.sort.spill.percent");
        value_text = new TiXmlText(SortPer.c_str());

        name->LinkEndChild(name_text);
        value->LinkEndChild(value_text);
        property->LinkEndChild(name);
        property->LinkEndChild(value);
        conf->LinkEndChild(property);


//====================================JvmReuse======================================
	property = new TiXmlElement("property");
        name = new TiXmlElement("name");
        value = new TiXmlElement("value");
        name_text = new TiXmlText("mapred.job.reuse.jvm.num.tasks");
        value_text = new TiXmlText(JvmReuse.c_str());

        name->LinkEndChild(name_text);
        value->LinkEndChild(value_text);
        property->LinkEndChild(name);
        property->LinkEndChild(value);
        conf->LinkEndChild(property);

//=======================================ShufflePer=================================
/*	
	property = new TiXmlElement("property");
        name = new TiXmlElement("name");
        value = new TiXmlElement("value");
        name_text = new TiXmlText("mapred.job.shuffle.merge.percent");
        value_text = new TiXmlText(ShufflePer.c_str());

        name->LinkEndChild(name_text);
        value->LinkEndChild(value_text);
        property->LinkEndChild(name);
        property->LinkEndChild(value);
        conf->LinkEndChild(property);

*/
	Conf_Xml.SaveFile();

	

	string search_string = "<?xml ?>";
  	string replace_string = "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?> ";
  	string inbuf;
  	fstream input_file("/home/trend-hadoop/expr/conf/conf_new/mapred-site.xml", ios::in);
  	ofstream output_file("/home/trend-hadoop/expr/conf/conf_new/mapred-site_tmp.xml");
	
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
		
	
	system("mv /home/trend-hadoop/expr/conf/conf_new/mapred-site_tmp.xml /home/trend-hadoop/expr/conf/conf_new/mapred-site.xml");


	
	return 0;
}
