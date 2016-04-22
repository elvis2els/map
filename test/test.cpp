#include <iostream>
#include <cmath>
#include <vector>

using namespace std;

int main()
{
    int num;
    cin >> num;
    vector<int> a;
    auto it = a.begin();
    double tag = log(num) / log(4) + 1.0e-7;
    cout << tag << " " << floor(tag) << endl;
    if(tag == floor(tag))
        cout << "True" << endl;
    else
        cout << "FALSE" << endl;
}
