package sqlbuilder

type timeExpression interface {
	expression

	EQ(rhs timeExpression) boolExpression
	NOT_EQ(rhs timeExpression) boolExpression
	LT(rhs timeExpression) boolExpression
	LT_EQ(rhs timeExpression) boolExpression
	GT(rhs timeExpression) boolExpression
	GT_EQ(rhs timeExpression) boolExpression
}

type timeInterfaceImpl struct {
	parent timeExpression
}

func (t *timeInterfaceImpl) EQ(rhs timeExpression) boolExpression {
	return EQ(t.parent, rhs)
}

func (t *timeInterfaceImpl) NOT_EQ(rhs timeExpression) boolExpression {
	return NOT_EQ(t.parent, rhs)
}

func (t *timeInterfaceImpl) LT(rhs timeExpression) boolExpression {
	return LT(t.parent, rhs)
}

func (t *timeInterfaceImpl) LT_EQ(rhs timeExpression) boolExpression {
	return LT_EQ(t.parent, rhs)
}

func (t *timeInterfaceImpl) GT(rhs timeExpression) boolExpression {
	return GT(t.parent, rhs)
}

func (t *timeInterfaceImpl) GT_EQ(rhs timeExpression) boolExpression {
	return GT_EQ(t.parent, rhs)
}

//---------------------------------------------------//
type prefixTimeExpression struct {
	expressionInterfaceImpl
	timeInterfaceImpl

	prefixOpExpression
}

func newPrefixTimeExpression(expression expression, operator string) timeExpression {
	timeExpr := prefixTimeExpression{}
	timeExpr.prefixOpExpression = newPrefixExpression(expression, operator)

	timeExpr.expressionInterfaceImpl.parent = &timeExpr
	timeExpr.timeInterfaceImpl.parent = &timeExpr

	return &timeExpr
}

func INTERVAL(interval string) expression {
	return newPrefixTimeExpression(Literal(interval), "INTERVAL")
}
