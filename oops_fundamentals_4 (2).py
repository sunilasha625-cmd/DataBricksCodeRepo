#user -> cc agent -> cost installation_util of product
#from pyspark.sql.session import SparkSession
#pkg/subpkg/module/class/obj/const
#functions (75%) fbp- pkg.subpkg.module.functions
#class/functions (25%) oops+fbp-pkg.subpkg.module.class.functions

#oops minimum concepts (class,members,self,obj,constructor)
#class - is a template or blueprint program contains related members func/variables/subclasses
#member - any var/funct/classes inside the class is a member
#self - Used to identify the given program as a member (it is the reserved first parameter)
#object - Memory Instance/Copy of a class
#Constructor () - Memory in which we  construct/instantiate a class is constructor
#Types of Constructor - Non Parameterized, Parameterized, Default

#class is a main program that holds the subprograms
#xls class (template/blueprint) holds subprograms (functions) (tabs)
class xls:
    def tab1(self):
        pass
    def tab2(self):
        pass

class prod_cost:
    def installation_cost(self):
        installation_cost=100+10
        return installation_cost
    def total_cost(self):
        total_cost=self.installation_cost()
        return total_cost

mohan_open=xls()
print(mohan_open.tab1())
karthick_open=xls()
print(karthick_open.tab1())
