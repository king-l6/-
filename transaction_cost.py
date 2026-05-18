"""
交易成本模型 — A股实盘成本计算

包含：
- 佣金（双向，默认万2.5，最低5元）
- 印花税（卖出单向，千分之一，2023年8月28日起降为千分之0.5）
- 过户费（沪市双向，万分之0.1）
- 滑点（可选，默认0.1%）

用法：
    from transaction_cost import TransactionCost
    tc = TransactionCost()
    net_return = tc.calc_net_return(buy_price, sell_price, market='sh')
"""


class TransactionCost:
    """A股交易成本计算器"""

    def __init__(
        self,
        commission_rate: float = 0.00025,    # 佣金费率 万2.5
        commission_min: float = 5.0,           # 最低佣金 5元
        stamp_tax_rate: float = 0.0005,        # 印花税 千分之0.5（2023.8.28起）
        transfer_fee_rate: float = 0.00001,    # 过户费 万分之0.1
        slippage_rate: float = 0.001,          # 滑点 0.1%
    ):
        self.commission_rate = commission_rate
        self.commission_min = commission_min
        self.stamp_tax_rate = stamp_tax_rate
        self.transfer_fee_rate = transfer_fee_rate
        self.slippage_rate = slippage_rate

    def calc_buy_cost(self, price: float, shares: int = 100) -> float:
        """计算买入成本（佣金 + 过户费 + 滑点），返回总费用"""
        amount = price * shares
        # 佣金
        commission = max(amount * self.commission_rate, self.commission_min)
        # 过户费
        transfer = amount * self.transfer_fee_rate
        # 滑点（实际买入价偏高）
        slippage = amount * self.slippage_rate
        return commission + transfer + slippage

    def calc_sell_cost(self, price: float, shares: int = 100) -> float:
        """计算卖出成本（佣金 + 印花税 + 过户费 + 滑点），返回总费用"""
        amount = price * shares
        # 佣金
        commission = max(amount * self.commission_rate, self.commission_min)
        # 印花税（卖出单向）
        stamp_tax = amount * self.stamp_tax_rate
        # 过户费
        transfer = amount * self.transfer_fee_rate
        # 滑点（实际卖出价偏低）
        slippage = amount * self.slippage_rate
        return commission + stamp_tax + transfer + slippage

    def calc_total_cost(self, buy_price: float, sell_price: float, shares: int = 100) -> float:
        """计算买卖总成本"""
        return self.calc_buy_cost(buy_price, shares) + self.calc_sell_cost(sell_price, shares)

    def calc_cost_rate(self, buy_price: float, sell_price: float) -> float:
        """计算成本占买入金额的比例（百分比）"""
        total_amount = buy_price * 100
        if total_amount <= 0:
            return 0.0
        cost = self.calc_total_cost(buy_price, sell_price, shares=100)
        return (cost / total_amount) * 100

    def calc_net_return(
        self,
        buy_price: float,
        sell_price: float,
        shares: int = 100
    ) -> dict:
        """
        计算扣除交易成本后的净收益

        返回:
            dict with:
                gross_return_pct: 毛收益率(%)
                cost_amount: 总交易成本(元)
                cost_rate: 成本占买入额比例(%)
                net_return_pct: 净收益率(%)
                net_profit: 净利润(元)
        """
        buy_amount = buy_price * shares
        sell_amount = sell_price * shares
        gross_profit = sell_amount - buy_amount
        gross_return_pct = (gross_profit / buy_amount * 100) if buy_amount > 0 else 0.0

        cost_amount = self.calc_total_cost(buy_price, sell_price, shares)
        cost_rate = (cost_amount / buy_amount * 100) if buy_amount > 0 else 0.0
        net_profit = gross_profit - cost_amount
        net_return_pct = (net_profit / buy_amount * 100) if buy_amount > 0 else 0.0

        return {
            'gross_return_pct': round(gross_return_pct, 2),
            'cost_amount': round(cost_amount, 2),
            'cost_rate': round(cost_rate, 4),
            'net_return_pct': round(net_return_pct, 2),
            'net_profit': round(net_profit, 2),
        }

    def adjust_buy_price(self, price: float) -> float:
        """调整买入价（加上滑点）"""
        return price * (1 + self.slippage_rate)

    def adjust_sell_price(self, price: float) -> float:
        """调整卖出价（减去滑点）"""
        return price * (1 - self.slippage_rate)

    def summary(self) -> str:
        """返回成本参数摘要"""
        return (
            f"佣金: 万{self.commission_rate * 10000:.1f}(最低{self.commission_min}元) | "
            f"印花税: 千{self.stamp_tax_rate * 1000:.1f} | "
            f"过户费: 万{self.transfer_fee_rate * 10000:.1f} | "
            f"滑点: {self.slippage_rate * 100:.2f}%"
        )


# 默认实例
default_tc = TransactionCost()


def calc_net_return(buy_price: float, sell_price: float, shares: int = 100) -> dict:
    """快捷函数：使用默认成本参数计算净收益"""
    return default_tc.calc_net_return(buy_price, sell_price, shares)
