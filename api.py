from flask import Flask, jsonify, request,json
app=Flask(__name__)

data=[{'name': 'harry'}, {'name':'terry'},{'name':'Larry'}]
@app.route('/',methods=['GET'])
def test():
    return jsonify({'message':'Voila! it works!'})

@app.route('/data',methods=['GET'])
def returnAll1():
    return jsonify({'data':data})
products=[{'product':'SUZUKI'},{'product':'Lamborghini'},{'product':'skoda'}]
@app.route('/products',methods=['GET'])
def product():
    return jsonify({'product':products})
@app.route('/products/<string:name>',methods=['GET'])
def returnproduct(name):
    product_found=[product for product in products if product['product']==name ]
    return jsonify({'product':product_found[0]})

@app.route('/products',methods=['POST'])
def Add_Product():
    abc={'product':request.json['product']}
    products.append(abc)
    return jsonify({'product':products})
@app.route('/products/<string:product>',methods=['DELETE'])
def Delete_Product(product):
    products.__delitem__(0)
    #products.append(abc)
    return jsonify({'product':products})




if __name__=='__main__':
    app.run(debug=True,port=8080)


