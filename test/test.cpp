#include <string>
#include <boost/date_time.hpp>
#include <fstream>

using namespace std;

vector<double> loadFromFile(std::string const & name)
{
    vector<double> ret;
    ifstream ifs(name);
    ifs.imbue(locale(ifs.getloc(), new boost::posix_time::time_input_facet("%Y-%m-%d %H:%M:%S")));
    double x;
    double y;
    boost::posix_time::ptime ptime;
    if (ifs)
    {
        while ( ifs.ignore(1024, ',') &&
                ifs.ignore(1024, ',') &&
                ifs >> ptime &&
                ifs.ignore(),
                ifs >> x && ifs.ignore() &&
                ifs >> y)
        {
            cout << "ptime: " << ptime << endl;
            cout << "x: " << x << endl;
            cout << "y: " << y << endl;
        }
    }
    return ret;
}

int main(int argc, char const *argv[])
{
    loadFromFile(argv[1]);
    return 0;
}
